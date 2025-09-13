import torch
import wandb
from dotenv import load_dotenv
import argparse
from transformers import AutoModel, AutoTokenizer
import torch.nn.functional as F
from datasets import load_dataset
from pathlib import Path
from tqdm import tqdm
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

assert load_dotenv(), "Failed to load environment variables from .env file"

sns.set_theme(style='whitegrid')

def parse_args():
    parser = argparse.ArgumentParser(description="Train a clustering model for affiliation clustering")

    parser.add_argument("--model", type=str, default="Alibaba-NLP/gte-multilingual-base",
                        help="Path to the model checkpoint")
    parser.add_argument("--learning_rate", type=float, default=1e-5,
                        help="Learning rate for the optimizer")
    parser.add_argument("--batch_size", type=int, default=32,
                        help="Batch size for training")
    parser.add_argument("--num_epochs", type=int, default=3,
                        help="Number of epochs for training")
    parser.add_argument("--margin", type=float, default=1.0,
                        help="Margin for triplet loss")
    parser.add_argument("--output_dir", type=str, default="checkpoints",
                        help="Directory to save model checkpoints")
    parser.add_argument("--warmup-steps", type=int, default=30,
                        help="Number of warmup steps for the learning rate scheduler")

    return parser.parse_args()


def get_embedding_class(args):
    class AffiliationEmbeddingModel(torch.nn.Module):
        def __init__(self):
            super(AffiliationEmbeddingModel, self).__init__()

            self.model = AutoModel.from_pretrained(args.model, device_map='auto', trust_remote_code=True)
            self.tokenizer = AutoTokenizer.from_pretrained(args.model, trust_remote_code=True)
            self.embedding_dim = 768

        def tokenize(self, input_texts):
            return self.tokenizer(
                input_texts,
                max_length=8192,
                padding=True,
                truncation=True,
                return_tensors='pt'
            ).to(self.model.device)

        def forward(self, **inputs):
            outputs = self.model(**inputs)
            embeddings = outputs.last_hidden_state[:, 0][:self.embedding_dim]
            embeddings = F.normalize(embeddings, p=2, dim=1)
            return embeddings

        def save_checkpoint(self, output_dir):
            self.model.save_pretrained(output_dir)
            self.tokenizer.save_pretrained(output_dir)

    return AffiliationEmbeddingModel


def main():
    args = parse_args()

    # load the model
    model_class = get_embedding_class(args)
    model = model_class()

    # load the data
    dataset = load_dataset("cometadata/triplet_loss_for_embedding_affiliations_sample_1")
    dataset = dataset['train'].train_test_split(test_size=0.1, seed=42)
    train_dataset = dataset['train'].shuffle(seed=42)
    test_dataset = dataset['test']

    # initialize wandb
    run = wandb.init(project="affiliation-clustering", config=args)
    wandb.watch(model, log="all")

    # training loop
    opt = torch.optim.AdamW(model.parameters(), lr=args.learning_rate)
    warmup_scheduler = torch.optim.lr_scheduler.ConstantLR(opt, factor=1e-5, total_iters=args.warmup_steps)
    cosine_scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
        opt,
        T_max=args.num_epochs * (1 + len(train_dataset) // args.batch_size),
        eta_min=0
    )
    scheduler = torch.optim.lr_scheduler.SequentialLR(
        opt,
        schedulers=[warmup_scheduler, cosine_scheduler],
        milestones=[args.warmup_steps]
    )
    loss_fn = torch.nn.TripletMarginLoss(margin=args.margin, p=2)
    model.train()

    for epoch_idx in range(args.num_epochs):
        for batch_start in tqdm(range(0, len(train_dataset), args.batch_size), desc=f"epoch {epoch_idx + 1}/{args.num_epochs}"):
            epoch_val = epoch_idx + batch_start / len(train_dataset)
            batch_end = min(batch_start + args.batch_size, len(train_dataset))
            batch = train_dataset[batch_start:batch_end]

            # get the embeddings
            anchor = model.tokenize(batch['anchor'])
            positive = model.tokenize(batch['positive'])
            negative = model.tokenize(batch['negative'])
            anchor_embeddings = model(**anchor)
            positive_embeddings = model(**positive)
            negative_embeddings = model(**negative)

            # loss
            loss = loss_fn(anchor_embeddings, positive_embeddings, negative_embeddings)

            # backpropagation
            opt.zero_grad()
            loss.backward()
            opt.step()
            scheduler.step()
            wandb.log({"train/loss": loss.item(), "train/epoch": epoch_val, "train/lr": opt.param_groups[0]['lr']})

        # save the model checkpoint
        ckpt_dir = Path(args.output_dir) / run.name / f"epoch_{epoch_idx}"
        ckpt_dir.mkdir(parents=True, exist_ok=True)
        model.save_checkpoint(ckpt_dir)
        print(f"Saved checkpoint for epoch {epoch_idx} at {ckpt_dir}")

        # test the model
        with torch.no_grad():
            test_loss = 0.0
            pos_scores = []
            neg_scores = []

            for batch_start in tqdm(range(0, len(test_dataset), args.batch_size), desc="testing"):
                batch_end = min(batch_start + args.batch_size, len(test_dataset))
                batch = test_dataset[batch_start:batch_end]

                # get the embeddings
                anchor = model.tokenize(batch['anchor'])
                positive = model.tokenize(batch['positive'])
                negative = model.tokenize(batch['negative'])
                anchor_embeddings = model(**anchor)
                positive_embeddings = model(**positive)
                negative_embeddings = model(**negative)

                # loss
                loss = loss_fn(anchor_embeddings, positive_embeddings, negative_embeddings)
                loss *= len(batch['anchor'])  # scale loss by batch size
                test_loss += loss.item()

                # calculate scores
                pos_scores_batch = [a @ p.T for a, p in zip(anchor_embeddings, positive_embeddings)]
                neg_scores_batch = [a @ n.T for a, n in zip(anchor_embeddings, negative_embeddings)]
                pos_scores.extend(pos_scores_batch)
                neg_scores.extend(neg_scores_batch)

            test_loss /= len(test_dataset)
            test_data = [
                [idx, a, p, n, p_score, n_score]
                for idx, (a, p, n, p_score, n_score) in enumerate(zip(
                    test_dataset['anchor'],
                    test_dataset['positive'],
                    test_dataset['negative'],
                    pos_scores,
                    neg_scores
                ))
            ]
            table = wandb.Table(data=test_data, columns=["index", "anchor", "positive", "negative", "positive_score", "negative_score"])
            wandb.log({"test/loss": test_loss, "test/scores": table})

            print(f"Test loss: {test_loss}")

            # visualize the triplet scores
            img_path = Path(args.output_dir) / run.name / f"epoch_{epoch_idx}" / "triplet_scores_distribution.png"
            df = pd.DataFrame({
                'positive_score': [score.item() for score in pos_scores],
                'negative_score': [score.item() for score in neg_scores]
            })
            sns.kdeplot(data=df)
            plt.title('Distribution of Triplet Scores')
            plt.xlabel('Triplet Score')
            plt.ylabel('Density')
            plt.savefig(img_path, dpi=300, bbox_inches='tight')
            wandb.log({"test/triplet_scores_distribution": wandb.Image(img_path)})
            plt.close()

    # finish wandb run
    wandb.finish()
    print("Training complete. Model checkpoints saved.")


if __name__ == "__main__":
    main()
