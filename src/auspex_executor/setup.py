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
            'controller_main_node = auspex_executor.controller.controller_handler:main',
            'executor_main_node = auspex_executor.platform_executor.executor:main',
            'kirk_server_handler = auspex_executor.platform_executor.executors.kirk_server_handler.kirk_server_handler:main',
        ],
    },
)
