import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="plumber-python-sdk",
    version="0.0.2",
    author="Bert Verstraete",
    author_email="bert.verstraete@ugent.be",
    description="An SDK built to ease interaction with the plumber sidecar in python",
    url="https://github.com/verstraetebert/plumber",
    classifiers=[
        "Programming Language :: Python :: 3.9",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)"
    ],
    package_dir={"": "src"},
    py_modules=["plumbersdk"],
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.9",
    install_requires=[
        'flask>=2.0.0',
        'cloudevents>=1.2.0'
    ]
)
