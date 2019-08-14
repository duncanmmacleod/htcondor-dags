#!/usr/bin/env python

from pathlib import Path
import argparse

import htcondor
import htcondor_dags as dags


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("src")
    parser.add_argument("dest")

    args = parser.parse_args()

    return args


def create_dag(source_dir, destination_dir):
    dag = dags.DAG()

    calculate_work = dag.layer(
        name="calculate_work",
        submit_description=htcondor.Submit(
            {
                "universe": "vanilla",
                "executable": "generate_file_listing.py",
                "arguments": f"{source_dir}",
                "output": "calculate_work.out",
                "error": "calculate_work.err",
                "log": "calculate_work.log",
                "should_transfer_files": "yes",
                "transfer_output_files": "source_manifest.txt",
                "requirements": 'Machine =?= "spaldingwcic0.chtc.wisc.edu"',
                "keep_claim_idle": 300,
                "+IS_TRANSFER_JOB": "true",
                "+WantFlocking": "true",
            }
        ),
        post=dags.Script(
            executable="write_subdag.py",
            arguments=[
                source_dir,
                "source_manifest.txt",
                destination_dir,
                destination_dir / "transfer_manifest.txt",
            ],
        ),
    )

    return dag


if __name__ == "__main__":
    args = parse_args()

    dag = create_dag(args)

    this_dir = Path(__file__).parent
    dag.write(this_dir, dag_file_name="sync.dag")
    print(dag.describe())
    print(f"Wrote DAG files to {this_dir}")
