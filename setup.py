import os
from setuptools import setup, find_packages

# Determine the absolute path to the requirements.txt file
base_dir = os.path.abspath(os.path.dirname(__file__))
requirements_path = os.path.join(base_dir, 'requirements.txt')

# Read the contents of the requirements.txt file
with open(requirements_path) as f:
    required = f.read().splitlines()

# Setup configuration for the Python package
setup(
    name='nemo_library',  # Name of the package
    version="1.6.41", 
    packages=find_packages(),  # Automatically find and include all packages
    install_requires=required,  # List of dependencies from requirements.txt
    include_package_data=True,
    package_data={
        'nemo_library': ['templates/migmantemplates.json'],
    },    
    author='Gunnar Schug',  # Author of the package
    author_email='GunnarSchug81@gmail.com',  # Author's email address
    description='A Python library for data management, project automation, and integration with the NEMO cloud solution',  # Updated short description
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    classifiers=[
        'Programming Language :: Python :: 3.13',  # Classifier indicating supported Python version
    ],
    project_urls={
        'Github': 'https://github.com/NEMOGunnar/nemo_library',  # URL to the project's GitHub repository
        'NEMO': 'https://enter.nemo-ai.com/nemo/'  # URL to the NEMO cloud solution
    },
    entry_points={
        'console_scripts': [
            'nemo-ui = nemo_library.ui.ui:main'
        ]
    },    
)