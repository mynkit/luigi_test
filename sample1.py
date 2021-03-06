'''
Reference:
    https://ohke.hateblo.jp/entry/2018/04/07/230000
files:
    .
    ├── sample1.py
    ├── Intermediate
    └── output

Abstract:
    * このスクリプトでやってること
        * Task1 -> ローカルの`intermediate/task1.txt`に現在時刻出力
        * Task2 -> ローカルの`intermediate/task1.txt`が存在すれば，その内容を`output/task2.txt`に追記

Memo:
    task_namespace
    self.input() -> self.input().open("r")で開ける．
        依存タスクのoutputのreturn．
        型は'luigi.local_target.LocalTarget'など
    self.output()
        自分のタスクのoutputのreturn
'''

import luigi
import time


class Task1(luigi.Task):
    task_namespace = 'tasks'

    def run(self):
        print("Task1: run")
        with self.output().open("w") as target:
            target.write(
                f"This file was generated by Task1 at {time.asctime()}.")

    def output(self):
        print("Task1: output")
        return luigi.LocalTarget("intermediate/task1.txt")


class Task2(luigi.Task):
    task_namespace = 'tasks'

    def requires(self):
        print("Task2: requires")
        return Task1()

    def run(self):
        print("Task2: run")
        with self.input().open("r") as intermediate, self.output().open("w") as target:
            task1_text = intermediate.read()

            target.write(f"{task1_text}\n")
            target.write(
                f"This file was generated by Task2 at {time.asctime()}.")

    def output(self):
        print("Task2: output")
        return luigi.LocalTarget("output/task2.txt")


if __name__ == '__main__':
    luigi.run(['tasks.Task2', '--workers', '1', '--local-scheduler'])
