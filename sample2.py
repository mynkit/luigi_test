'''
Reference:
    https://dev.classmethod.jp/tool/advent_calendar_2016_luigi_12_08_save_s3/

files:
    .
    ├── sample2.py
    └── output/test.txt

Abstract:
    * このスクリプトでやってること
        * createTargetFile -> ローカルの`station/test.txt`に現在時刻を吐き出す
        * putS3 -> ローカルの`station/test.txt`が存在したら，`s3://prd-data-store/station/test.txt`にローカルの`station/test.txt`の内容を吐き出す

Usage:
    AWS_PROFILE=default pipenv run python sample2.py

Memo:
    luigi.buildはよくわからない
    luigi.contrib.s3を使うためにはboto3が必要

'''

import luigi
from luigi.contrib.s3 import S3Target
import datetime
 
 
class createTargetFile(luigi.Task):
    task_namespace = 'tasks'
    fileName = luigi.Parameter()
 
    def output(self):
        return luigi.LocalTarget(self.fileName)
 
    def run(self):
        with self.output().open('w') as out_file:
            out_file.write(datetime.datetime.now().isoformat())
class putS3(luigi.Task):
    task_namespace = 'tasks'
 
    bucketName = luigi.Parameter(default="prd-data-store")
    fileName = luigi.Parameter(default="station/test.txt")
 
    def requires(self):
        return createTargetFile(self.fileName)
 
    def output(self):
        return S3Target("s3://{}/{}".format(self.bucketName, self.fileName))
 
    def run(self):
        with self.input().open('r') as input, self.output().open('w') as out_file:
            for line in input:
                out_file.write(line)


if __name__ == '__main__':
    luigi.run(['tasks.putS3', '--workers', '1', '--local-scheduler'])
    # luigi.build([putS3(bucketName='prd-data-store', fileName='station/test.txt')])