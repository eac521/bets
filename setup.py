
from setuptools import setup, find_packages

from pathlib import Path
long_description = "NBA Betting Analysis and Prediction System"

setup(
    # Basic package information
    name="nba-betting",
    version="0.1.0",
    description="Leveraging nba_api to pull data, storing in dropbox and using models for predictions",
    long_description=long_description,
    long_description_content_type="text/markdown",
    
    
    # Package discovery
    packages=find_packages(),
    
    # Dependencies 
    install_requires=[
        "pandas>=1.3.0",
        "numpy>=1.20.0",
        "scikit-learn>=1.0.0",
        "requests>=2.25.0",
        "nba-api>=1.1.0",
        "tqdm>=4.60.0",
    ],
    
    # Optional dependencies for development/notebooks
     extras_require={
        "nba": ["nba-api>=1.1.0"],
        "mlb": ["pybaseball>=2.0.0"],  
        "nfl": ["nfl-data-py>=0.1.0"],  
        "dev": [
            "jupyter>=1.0.0",
            "matplotlib>=3.3.0",
            "seaborn>=0.11.0",
        ],
    },
    
    # Python version requirement
    python_requires=">=3.8",
    
)
