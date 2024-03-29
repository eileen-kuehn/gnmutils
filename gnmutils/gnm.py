"""
This module manages the different possibilities to start and work with GNM tool output.
Conversion of different file types, etc.
"""
import argparse
import multiprocessing
import logging
import os
import re

from gnmutils.sources.datasource import DataSource
from gnmutils.sources.filedatasource import FileDataSource
from gnmutils.sources.dbbackedfiledatasource import DBBackedFileDataSource
from gnmutils.pilot import Pilot
from gnmutils.utils import directory_level, relevant_directories, RUN_LEVEL

from utility.report import LVL, update_parser, argparse_init
from utility.exceptions import ExceptionFrame, mainExceptionFrame


def create_payloads():
    """
    For CMS pilots there is a very simple (but quick-n-dirty) approach to recognise the actual
    payloads. The method is based on the name of processes. As soon as there is a reliable but
    automatic solution, I need to switch to this one.

    The extractor looks into the different CMS jobs, which payloads still have not been identified
    and extracts those data. It is saved to `processed/payloads`.

    The payload ids are build from the job id from database and additionally the payload count.
    """
    print
    print "Starting to extract payloads from CMS pilots"
    path = eval_input_path()
    output_path = eval_output_path()
    count = eval_cores()
    level = directory_level(path)
    if level == RUN_LEVEL:
        count = 1

    do_multicore(
        count=count,
        target=_create_payloads,
        data=[{"path": os.path.join(os.path.join(element[0], element[1]), element[2]),
               "output_path": output_path} for element in list(relevant_directories(path))])


def import_cms_dashboard_data():
    """
    Method that starts the import of data from CMS dashboard.
    """
    print
    print "Starting to import cms data"
    path = eval_input_path()
    data_source = DBBackedFileDataSource()
    for data in data_source.object_data(path=path):
        if data is not None:
            for key in data:
                for result in data[key]:
                    data_source.write_payload_result(
                        data=result,
                        workernode=re.match("(^c\d{2}-\d{3}-\d{3})", result["WNHostName"]).group(1),
                        activity=key)


def prepare_raw_data():
    """
    Function that starts the splitting of raw data that was directly retrieved by GNM tool.
    """
    print
    print "Starting to split data stream into jobs"
    path = eval_input_path()
    output_path = eval_output_path()
    count = eval_cores()
    level = directory_level(path)
    if level == RUN_LEVEL:
        count = 1
    do_multicore(
        count=count,
        target=_prepare_raw_data,
        data=[{"path": os.path.join(os.path.join(element[0], element[1]), element[2]),
               "output_path": output_path} for element in list(relevant_directories(path))])


def archive_jobs():
    """
    Function that starts archival of written jobs into a single zip file.
    """
    print
    print "Starting to archive valid and complete jobs"
    path = eval_input_path()
    count = eval_cores()
    names = ["path", "workernode", "run"]
    data = [dict(zip(names, element)) for element in list(relevant_directories(path))]
    do_multicore(
        count=count,
        target=_archive_jobs,
        data=data)


def generate_network_statistics():
    """
    Function that starts generation of network statistics based on available raw data.
    """
    print
    print "Starting to generate network statistics"
    path = eval_input_path()
    output_path = eval_output_path()
    count = eval_cores()
    level = directory_level(path)
    if level == RUN_LEVEL:
        count = 1
    do_multicore(
        count=count,
        target=_generate_network_statistics,
        data=[{"path": os.path.join(os.path.join(element[0], element[1]), element[2]),
               "output_path": output_path} for element in list(relevant_directories(path))]
    )


def do_multicore(count=1, target=None, data=None):
    """
    Entry function that starts the actual functions using given :py:attr:`count` of cores.

    :param count: number of cores to use for processing
    :param target: target function to use for processing
    :param data: data to use for processing
    """
    pool = multiprocessing.Pool(processes=count)
    pool.map(target, data)
    pool.close()
    pool.join()


def _create_payloads(args):
    with ExceptionFrame():
        data_source = DataSource.best_available_data_source()
        path = args.get("path", None)
        output_path = args.get("output_path", None)
        for pilot in data_source.jobs(path=path):
            pilot.__class__ = Pilot
            if pilot.is_cms_pilot():
                pilot.prepare_traffic()
                for payload, _ in pilot.payloads():
                    # write file per payload
                    data_source.write_payload(path=output_path,
                                              data=payload)
            else:
                logging.info("current pilot is not a CMS pilot")


def _archive_jobs(args):
    with ExceptionFrame():
        data_source = DataSource.best_available_data_source()
        path = args.get("path", None)
        workernode = args.get("workernode", None)
        run = args.get("run", None)
        current_path = os.path.join(os.path.join(path, workernode), run)
        for job in data_source.jobs(path=current_path):
            if job.is_complete() and job.is_valid():
                data_source.archive(data=job, path=path, name="jobarchive")


def _generate_network_statistics(kwargs):
    with ExceptionFrame():
        data_source = FileDataSource()
        path = kwargs.get("path", None)
        output_path = kwargs.get("output_path", None)
        for stats in data_source.network_statistics(path=path,
                                                    stateful=True):
            data_source.write_network_statistics(data=stats, path=output_path)


def _prepare_raw_data(kwargs):
    with ExceptionFrame():
        path = kwargs.get("path", None)
        output_path = kwargs.get("output_path", None)
        data_source = DataSource.best_available_data_source()
        for job in data_source.jobs(source="raw",
                                    path=path,
                                    data_path=output_path,
                                    stateful=True):
            data_source.write_job(data=job, path=output_path)
        for traffic in data_source.traffics(source="raw",
                                            path=path,
                                            data_path=output_path,
                                            stateful=True):
            data_source.write_traffic(data=traffic, path=output_path)


def eval_options_choice():
    """
    Function that presents available workflows and takes the input from user.
    """
    print "What do you want to do now?"
    print "\t1. split data stream into jobs"
    print "\t2. extract payloads from jobs (currently just supported for CMS)"
    print
    print "More workflows:"
    print "\ta) put jobs into archive"
    print "\tb) import CMS dashboard data"
    print "\tc) generate data on byte size and number of events per interval"
    print
    return_options(raw_input("Please choose: "))()


def eval_input_path():
    """
    Function that takes the input path from the user.

    :return: input path
    """
    input_path = raw_input("Input Path: ")
    return input_path


def eval_output_path():
    """
    Function that takes the output path from the user.

    :return: output path
    """
    output_path = raw_input("Output Path: ")
    return output_path


def eval_cores():
    """
    Function that takes the number of cores to use from the user.

    :return: number of cores, else 1
    """
    try:
        return int(raw_input("Number of cores for processing (%d): " % multiprocessing.cpu_count()))
    except:
        return 1


def return_options(input):
    """
    Function that evaluates the given input by user and maps to available workflows.

    :param input: given input
    :return: selected function signature
    """
    print input
    return {
        '1': prepare_raw_data,
        '2': create_payloads,
        'a': archive_jobs,
        'b': import_cms_dashboard_data,
        'c': generate_network_statistics,
        'q': exit
    }.get(input, eval_options_choice)


def main():
    """
    Main starting point.
    """
    print "You started the main process for GNM workflows."
    eval_options_choice()

if __name__ == '__main__':
    command_line_interface = argparse.ArgumentParser()
    update_parser(command_line_interface)
    argparse_init(command_line_interface.parse_args())

    logging.getLogger().setLevel(LVL.WARNING)
    logging.getLogger("EXCEPTION").setLevel(LVL.INFO)
    mainExceptionFrame(main)
