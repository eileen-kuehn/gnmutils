import re
import os
import argparse
import multiprocessing

from utility.exceptions import *
from utility.report import LVL, update_parser, argparse_init

from gnmutils.sources.datasource import DataSource
from gnmutils.sources.filedatasource import FileDataSource
from gnmutils.sources.dbbackedfiledatasource import DBBackedFileDataSource
from gnmutils.pilot import Pilot
from evenmoreutils import path as pathutils


def create_payloads():
    """
    For CMS pilots there is a very simple (but quick-n-dirty) approach to recognise the actual payloads. The method
    is based on the name of processes. As soon as there is a realiable but automatical solution, I need to switch
    to this one.

    The extractor looks into the different CMS jobs, which payloads still have not been identified and extracts those
    data. It is saved to `processed/payloads`.

    The payload ids are build from the job id from database and additionally the payload count.
    """
    data_source = FileDataSource()

    for pilot in data_source.jobs():
        pilot.__class__ = Pilot
        for payload, count in pilot.payloads():
            # write file per payload
            data_source.write_payload(path="/Users/eileen/projects/Dissertation/Development/data/processed",
                                      data=payload)


def import_cms_dashboard_data():
    data_source = DBBackedFileDataSource()
    for data in data_source.object_data(path="/Users/eileen/Development/git/KIT/cmsdataextender"):
        for key in data:
            for result in data[key]:
                payload_result_object = data_source.write_payload_result(
                    data=result,
                    workernode=re.match("(^c\d{2}-\d{3}-\d{3})", result["WNHostName"]).group(1),
                    activity=key)


def prepare_raw_data():
    print
    print("Starting to split data stream into jobs")
    path, output_path = eval_input_output_path()
    count = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=count)
    pool.map(_prepare_raw_data, [{
        "path": os.path.join(path, workernode),
        "output_path": os.path.join(output_path, workernode)
    } for workernode in pathutils.getImmediateSubdirectories(path,
                                                             pattern="c(\d)*-(\d)*-(\d)*")])


def _prepare_raw_data(args):
    path = args.get("path", None)
    output_path = args.get("output_path", None)
    data_source = DataSource.best_available_data_source()
    for job in data_source.jobs(source="raw",
                                path=path,
                                output_path=output_path,
                                archive=True):
        job = data_source.write_job(data=job,
                                    path=output_path)


def split_jobs():
    pass


def eval_options_choice():
    print("What do you want to do now?")
    print("\t1. split data stream into jobs")
    print("\t2. extract payloads from jobs (currently just supported for CMS)")
    print
    print("More workflows:")
    print("\ta) split job into processes and traffic")
    print("\tb) import CMS dashboard data")
    print
    return_options(raw_input("Please choose: "))()


def eval_input_output_path():
    input_path = raw_input("Input folder: ")
    output_path = raw_input("Output folder: ")
    return input_path, output_path


def return_options(x):
    print(x)
    return {
        '1': prepare_raw_data,
        '2': create_payloads,
        'a': split_jobs,
        'b': import_cms_dashboard_data,
        'q': exit
    }.get(x, eval_options_choice)


def main():
    print("You started the main process for GNM workflows.")
    eval_options_choice()

if __name__ == '__main__':
    cli = argparse.ArgumentParser()
    update_parser(cli)
    argparse_init(cli.parse_args())

    logging.getLogger().setLevel(LVL.WARNING)
    logging.getLogger("EXCEPTION").setLevel(LVL.INFO)
    mainExceptionFrame(main)
