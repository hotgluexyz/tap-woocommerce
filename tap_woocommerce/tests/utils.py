def compare_dicts(dict1, dict2):
    # Check if both are dictionaries
    if isinstance(dict1, dict) and isinstance(dict2, dict):
        # Check if they have the same keys
        if dict1.keys() != dict2.keys():
            return False

        # Recursively compare each key-value pair
        for key in dict1:
            if not compare_dicts(dict1[key], dict2[key]):
                return False
        return True
    # If they are lists, compare each item
    elif isinstance(dict1, list) and isinstance(dict2, list):
        if len(dict1) != len(dict2):
            return False
        for item1, item2 in zip(dict1, dict2):
            if not compare_dicts(item1, item2):
                return False
        return True
    # For simple types (int, str, etc.), directly compare
    else:
        return dict1 == dict2
