from setuptools import setup, find_packages


setup(name = "tap-faethm",
    version = 0.1,
    description = "Meltano tap to pull data from Faethm into Degreed",

    author = "Degreed",
    author_email = "",
    url = "https://github.com/degreed-data-engineering/tap-faethm",
    packages = find_packages(),
    package_data = {},
    include_package_data = True,
    classifiers=[
      'Development Status :: 3 - Alpha',
      'Intended Audience :: Science/Research',
      'Operating System :: OS Independent',
      'Programming Language :: Python',
      'Topic :: Scientific/Engineering :: Astronomy'
      ],
      install_requires=[
        "requests>=2.25.1",
        # ... other dependencies ...
    ],
    zip_safe=False
)