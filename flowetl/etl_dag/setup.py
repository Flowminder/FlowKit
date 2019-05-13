from setuptools import find_packages, setup

setup(
    name="etl-dag",
    url="https://github.com/Flowminder/FlowKit",
    maintainer="Flowminder",
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=["apache-airflow==1.10.3"],
)
