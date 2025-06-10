from setuptools import find_packages, setup
import os
from glob import glob

package_name = 'auspex_executor'

setup(
    name=package_name,
    version='1.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages',['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        (os.path.join('share', package_name, 'launch'), glob('launch/*.launch.py'))
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='Bjoern Doeschl',
    maintainer_email='bjoern.doeschl@unibw.de',
    description='AUSPEX Executor Package',
    license='MIT',
    entry_points={
        'console_scripts': [
            'executor_main_node = auspex_executor.executor_handler:main',
            'test_node = auspex_executor.test_node:main'
        ],
    },
)
