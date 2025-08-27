import re
import unicodedata
import jellyfish
from nameparser import HumanName


def parse_name_by_style(name, style):
    name = name.strip()

    if style == 'last_initial':
        parts = name.split()
        if len(parts) >= 2:
            last_name = ' '.join(parts[:-1])
            initials = parts[-1]
            first_initial = initials[0].lower() if initials else ''
            return {'first': first_initial, 'last': last_name.lower(), 'middle': '', 'normalized': f"{last_name.lower()} {first_initial}", 'original': name, 'style': style}
        else:
            return {'first': '', 'last': name.lower(), 'middle': '', 'normalized': name.lower(), 'original': name, 'style': style}

    elif style == 'last_comma_first':
        if ',' in name:
            parts = name.split(',', 1)
            last = parts[0].strip()
            rest = parts[1].strip() if len(parts) > 1 else ''
            rest_parts = rest.split()
            first = rest_parts[0].lower() if rest_parts else ''
            middle = ' '.join(rest_parts[1:]).lower() if len(rest_parts) > 1 else ''
            return {'first': first, 'last': last.lower(), 'middle': middle, 'normalized': f"{first} {middle} {last.lower()}".strip(), 'original': name, 'style': style}

    elif style == 'last_first':
        parts = name.split()
        if len(parts) >= 2:
            last = parts[0]
            first = parts[1] if len(parts) > 1 else ''
            middle = ' '.join(parts[2:]) if len(parts) > 2 else ''
            return {'first': first.lower(), 'last': last.lower(), 'middle': middle.lower(), 'normalized': f"{first.lower()} {middle.lower()} {last.lower()}".strip(), 'original': name, 'style': style}

    elif style == 'first_initial_last':
        parts = name.split()
        initials = []
        last_idx = -1
        for i, part in enumerate(parts):
            if len(part) <= 2 and (part.endswith('.') or len(part) == 1):
                initials.append(part.replace('.', '').lower())
            else:
                last_idx = i
                break
        if last_idx >= 0:
            last = ' '.join(parts[last_idx:])
            first = initials[0] if initials else ''
            middle = ' '.join(initials[1:]) if len(initials) > 1 else ''
            return {'first': first, 'last': last.lower(), 'middle': middle, 'normalized': f"{first} {middle} {last.lower()}".strip(), 'original': name, 'style': style}
    
    parsed = HumanName(name)
    first = (parsed.first or '').strip()
    last = (parsed.last or '').strip()
    middle = (parsed.middle or '').strip()
    clean = f"{first} {middle} {last}".strip()
    clean = unicodedata.normalize('NFKD', clean).encode('ascii', 'ignore').decode()
    normalized = re.sub(r'[-.,]', ' ', clean.lower()).strip()
    return {'first': first.lower(), 'last': last.lower(), 'middle': middle.lower(), 'normalized': normalized, 'original': name, 'style': 'first_last'}


def are_names_similar(name1_str, name2_str, name1_style='auto', name2_style='auto', threshold=0.85):
    name1 = parse_name_by_style(name1_str, name1_style)
    name2 = parse_name_by_style(name2_str, name2_style)
    if not name1['last'] or not name2['last']:
        return name1['normalized'] == name2['normalized']
    last_similarity = jellyfish.jaro_winkler_similarity(name1['last'], name2['last'])
    if last_similarity < threshold:
        return False
    if name1['first'] and name2['first']:
        if len(name1['first']) == 1 or len(name2['first']) == 1:
            if name1['first'][0] == name2['first'][0]:
                return True
        else:
            first_similarity = jellyfish.jaro_winkler_similarity(name1['first'], name2['first'])
            if first_similarity >= threshold:
                return True
    if last_similarity >= 0.95:
        return True
    return False