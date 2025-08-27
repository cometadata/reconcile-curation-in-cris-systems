from flair.nn import Classifier
from flair.data import Sentence


class EntityExtractor:
    
    def __init__(self):
        try:
            self.model = Classifier.load('flair/ner-english-fast')
            print("NER model loaded successfully.")
        except Exception as e:
            print(f"Warning: Could not load NER model: {e}")
            self.model = None
    
    def extract_organizations(self, text):
        if not text or not self.model:
            return []
        
        try:
            sentence = Sentence(text)
            self.model.predict(sentence)
            
            organizations = []
            for entity in sentence.get_spans('ner'):
                if entity.tag == 'ORG':
                    organizations.append(entity.text)
            
            return organizations
        except Exception as e:
            print(f"Warning: Error extracting organizations from text: {e}")
            return []
    
    def extract_and_validate_from_affiliations(self, unique_affiliations, original_affiliations_map):
        if not self.model:
            return []
        
        extracted_entities = []
        
        valid_affiliations = []
        affiliation_to_keys = {}
        
        for norm_affil, orig_affil in original_affiliations_map.items():
            if not orig_affil:
                continue
            
            if orig_affil not in affiliation_to_keys:
                affiliation_to_keys[orig_affil] = []
                valid_affiliations.append(orig_affil)
            
            affiliation_to_keys[orig_affil].append(norm_affil)
        
        if not valid_affiliations:
            return extracted_entities
        
        sentences = [Sentence(affil) for affil in valid_affiliations]
        
        try:
            self.model.predict(sentences)
        except Exception as e:
            print(f"Warning: Error during batch NER prediction: {e}")
            return extracted_entities
        
        for i, sentence in enumerate(sentences):
            orig_affil = valid_affiliations[i]
            
            for entity in sentence.get_spans('ner'):
                if entity.tag == 'ORG':
                    extracted_entities.append((entity.text, orig_affil))
        
        return extracted_entities