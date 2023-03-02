import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name='pypee',
    version='0.0.6',
    author='Chad Wood',
    author_email='chadwood.jds@gmail.com',
    description='A Python ETL Pipeline Package.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/MrChadMWood/pypee',
    project_urls = {
        "Bug Tracker": "https://github.com/MrChadMWood/pypee/issues"
    },
    license='GPL-3.0-or-later',
    packages=['pypee'],
    install_requires=[
        'enum',
        'traceback',
        'functools',
        'types',
        'typing',
        'pandas',
        'concurrent.futures',
        'textwrap',
        'json',
        're',
    ],
)
