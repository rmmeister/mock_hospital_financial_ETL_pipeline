import os

# Auto-detects your root path dynamically
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

def path(*parts):
    """Helper to construct absolute paths based on the project root"""
    return os.path.join(PROJECT_ROOT, *parts)