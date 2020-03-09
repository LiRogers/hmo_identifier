from setuptools import find_packages, setup
from dotenv import load_dotenv

setup(
    name='hmo_identifier',
    packages=find_packages(),
    version='0.1.0',
    description='Identify Houses of Multiple Occupation',
    author='Libby Rogers',
    license='MIT',
)

load_dotenv()