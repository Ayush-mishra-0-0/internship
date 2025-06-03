def preprocess_text(content: str) -> str:
    """
    Example preprocessing function.
    
    Args:
        content (str): Input file content.
    
    Returns:
        str: Processed content.
    """
    # Example: Convert text to uppercase and remove extra spaces
    processed = ' '.join(content.split()).upper()
    return processed