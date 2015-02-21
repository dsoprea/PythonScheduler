import setuptools
import os

import scheduler

app_path = os.path.dirname(scheduler.__file__)

with open(os.path.join(app_path, 'resources', 'README.rst')) as f:
      long_description = f.read()

with open(os.path.join(app_path, 'resources', 'requirements.txt')) as f:
      install_requires = list(map(lambda s: s.strip(), f.readlines()))

setuptools.setup(
      name='scheduler',
      version=scheduler.__version__,
      description="Multithreaded Python-routine scheduling framework",
      long_description=long_description,
      classifiers=[],
      license='GPL 2',
      keywords='tasks jobs schedule scheduling',
      author='Dustin Oprea',
      author_email='myselfasunder@gmail.com',
      packages=setuptools.find_packages(exclude=['dev']),
      include_package_data=True,
      zip_safe=False,
      install_requires=install_requires,
      package_data={
            'scheduler': ['resources/README.rst',
                          'resources/requirements.txt',
                          'resources/scripts/*'],
      },
      scripts=[
            'scheduler/resources/scripts/sched_process_tasks_prod',
            'scheduler/resources/scripts/sched_process_tasks_dev',
      ],
)
