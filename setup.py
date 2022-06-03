from setuptools import setup, find_packages

setup(
    name='cleanrooom',
    packages = find_packages(),
    install_requires = ["mlflow", "delta-spark", "ipython", "databricks-cli"]
)
