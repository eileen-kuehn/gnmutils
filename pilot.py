from gnmutils.job import Job
from evenmoreutils.tree import Tree


class Pilot(Job):
    """
    The class Pilot adds additional methods to split a pilot into its Payloads.

    Attention: This is currently just working for CMS in a rather static way.
    """
    def __init__(self):
        Job.__init__(self)

    def payloads(self, exclude_watchdog=False):
        if self.tree is not None:
            count = 0
            for node, depth in self.tree.walkDFS():
                if "condor_startd" in node.value.name:
                    # found job root
                    for child in node.children:
                        if "condor_starter" in child.value.name:
                            root_set = False
                            # payload found
                            # generate new job object
                            count += 1
                            job = Job(job_id="%s-%d" % (self._job_id, count),
                                      data_source=self._data_source)
                            for child_node, child_depth in Tree(child).walkDFS():
                                if exclude_watchdog:
                                    if "crabWatchdog.sh" in child_node.value.name:
                                        # add watchdog and skip childs
                                        job.add_process(child_node.value)
                                        found_watchdog = True
                                        watchdog_depth = child_depth
                                        continue
                                    if found_watchdog:
                                        # check if the depth is smaller/equal than watchdog depth
                                        if depth <= watchdog_depth:
                                            found_watchdog = False
                                        else:
                                            continue
                                if not root_set:
                                    job.add_process(process=child_node.value, is_root=True)
                                    root_set = True
                                else:
                                    job.add_process(child_node.value)
                            yield job, count
