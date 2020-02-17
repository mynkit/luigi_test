'''
Reference:
    https://dev.classmethod.jp/tool/luigi_trial/
files:
    .
    └── hello_world.py
'''

import luigi
from luigi.mock import MockTarget


class HelloWorldTask(luigi.Task):
    task_namespace = 'examples'

    def requires(self):
        return WorldIsFineTask()

    def run(self):
        print("{task} says: Hello world!".format(task=self.__class__.__name__))


class WorldIsFineTask(luigi.Task):
    task_namespace = 'examples'

    def run(self):
        with self.output().open('w') as output:
            print("{task} says: The world is a fine place!".format(
                task=self.__class__.__name__))
            # output.write('{task} says: The world is a fine place!\n'.format(task=self.__class__.__name__))

    def output(self):
        return MockTarget("output")
        # return luigi.LocalTarget('world.txt')


if __name__ == '__main__':
    luigi.run(['examples.HelloWorldTask', '--workers', '1', '--local-scheduler'])
