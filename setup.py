from setuptools import setup, find_packages


setup(
    name = 'duffel',
    version = '0.1.3',
    description = 'Lightweight Python data frames on the standard library',
    long_description = 'Lightweight Python data frames without bloat or typecasting, using only the standard library.',
    keywords = 'pandas numpy python dflite data.frame dataframe data lambda aws',
    url = 'https://github.com/russellromney/duffel',
    download_url = 'https://github.com/russellromney/duffel/archive/v0.1.3.tar.gz',
    author = 'Russell Romney',
    author_email = 'russellromney@gmail.com',
    license = 'MIT',
    packages = find_packages(),
    install_requires = [],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',        
        ],
    include_package_data = False,
    zip_safe = False
)
