#!/usr/bin/env python
#
# launch script for CHPC
# deals with both command files for parametric launcher
# and with single commands

import argparse
import os
import sys
from tempfile import *
import subprocess
import math
from datetime import datetime

CORES = {'normal': 48, 'largemem': 32, 'hugemem': 20,
         'development': 48, 'gpu': 10, 'largemem512GB': 64,
         'skx-normal': 48}
MAXNODES = {'normal': 171, 'largemem': 342, 'hugemem': 2,
            'development': 11, 'gpu': 4, 'largemem512GB': 4,
            'skx-normal': 128}
MAXCORES = {'normal': 4104, 'largemem': 8208, 'hugemem': 40,
            'development': 264, 'gpu': 40, 'largemem512GB': 4 * 64,
            'skx-normal': 3072}

def launch_torque(cmd='', script_name=None, runtime='01:00:00',
                 jobname='launch', outfile=None, projname=None,
                 email=None, qsubfile=None, keepqsubfile=False,
                 test=False, compiler='intel', hold=None, cwd=None,
                 nnodes=None, tpn=None, mem=None,
                 schedule='dynamic'):

    if len(cmd) > 0:
        parametric = False
        print('Running serial command: ' + cmd)
        ncmds = 1
        nnodes = 1
    elif script_name is not None:
        # read commands file
        try:
            f = open(script_name, 'r')
        except:
            print('%s does not exist!' % script_name)
            sys.exit(0)
        script_cmds = f.readlines()
        f.close()

        # check for empty lines
        for s in script_cmds:
            if s.strip() == '':
                print(
                    'command file contains empty lines - please remove them first')
                sys.exit()

        # determine whether to use launcher
        ncmds = len(script_cmds)
        print('found %d commands' % ncmds)
        if ncmds == 1:
            # if only one, do not use launcher, which fails sometimes
            parametric = False
            cmd = script_cmds[0]
            print('Running serial command: ' + cmd)
        else:
            parametric = True
            print('Submitting parametric job file: ' + script_name)

    else:
        print(
            'ERROR: you must either specify a script name (using -s) or a command to run\n\n')
        sys.exit()

    if qsubfile is None:
        qsubfile, qsubfilepath = mkstemp(prefix=jobname + "_",
                                         dir='.', suffix='.slurm', text=True)
        os.close(qsubfile)
    else:
        qsubfilepath = qsubfile

    print('Outputting SLURM commands to %s' % qsubfilepath)
    qsubfile = open(qsubfilepath, 'w')
    qsubfile.write('#!/bin/bash\n#\n')
    qsubfile.write('# TORQUE control file automatically created by launch\n#\n')
    qsubfile.write('# Created on: {}\n'.format(datetime.now()))
    if parametric:
        # fill in the blanks
        if tpn is not None:
            # user specified the number of tasks per node; get the
            # number of nodes given that, evenly splitting tasks by
            # node
            nnodes = int(math.ceil(float(ncmds) / float(tpn)))
            ntasks = nnodes * int(tpn)
        elif ntasks is None and nnodes is not None:
            # tasks and tpn not specified; set tasks to the number of
            # commands
            nnodes = int(nnodes)
            ntasks = nnodes * float(CORES[queue])
            print("Number of tasks not specified; estimated as %d" % ntasks)
        else:
            nnodes = int(nnodes)
            #ntasks = int(ntasks)

        # if queue in MAXNODES:
        #     if nnodes > MAXNODES[queue]:
        #         nnodes = MAXNODES[queue]
        # if queue in MAXCORES:
        #     if ntasks > MAXCORES[queue]:
        #         ntasks = MAXCORES[queue]

        qsubfile.write(
            '# Using parametric launcher with control file: %s\n#\n' % script_name)
        if tpn == None:
            qsubfile.write('#PBS -l nodes=%d,walltime=%s\n' % *(nnodes,runtime))
        else:
            qsubfile.write('#PBS -l nodes=%d:ppn=%d,walltime=%s\n' % (nnodes,tpn,runtime))
    else:
        qsubfile.write('# Launching single command: %s\n#\n' % cmd)
        qsubfile.write('#PBS -l nodes=1,walltime=%s\n' %runtime)
        #qsubfile.write('#SBATCH -n 1\n')

    if cwd is not None:
        qsubfile.write('#PBS -d %s\n' % cwd)
    qsubfile.write('#PBS -N %s\n' % jobname)
    if outfile is not None:
        qsubfile.write('#PBS -j oe {0}\n'.format(outfile))
    else:
        qsubfile.write('#PBS -j oe {0}.o%j\n'.format(jobname))
    # qsubfile.write('#PBS -q %s\n' % queue)

    if type(hold) is str:
        qsubfile.write("#PBS -d afterok")
        qsubfile.write(":{0}".format(int(hold)))
        qsubfile.write('\n')

    if projname is not None:
        qsubfile.write("#SBATCH -A {0}\n".format(projname))

    if email is not None:
        qsubfile.write('#PBS -M %s\n' % email)

    qsubfile.write('\numask 2\n\n')

    if cwd is None:
        cwd = os.getcwd()

    qsubfile.write('echo " Starting at $(date)"\n')
    qsubfile.write('start=$(date +%s)\n')
    qsubfile.write('echo " WORKING DIR: %s/"\n' % cwd)
    qsubfile.write('echo " JOB ID:      $PBS_JOBID"\n')
    qsubfile.write('echo " JOB NAME:    $PBS_JOBNAME"\n')
    qsubfile.write('echo " NODES:       $PBS_NODEFILE"\n')
    qsubfile.write('echo " N NODES:     $PBS_NUM_NODES"\n')
    qsubfile.write('echo " N TASKS:     $PBS_NUM_PPN"\n')

    if compiler == "gcc":
        qsubfile.write('module swap intel gcc\n')

    else:
        qsubfile.write('export LAUNCHER_SCHED=%s\n' % schedule)
        qsubfile.write('export LAUNCHER_JOB_FILE=%s\n' % script_name)
        if cwd is not None:
            qsubfile.write('export LAUNCHER_WORKDIR=%s\n' % cwd)
        else:
            qsubfile.write('export LAUNCHER_WORKDIR=$(pwd)\n')
        qsubfile.write('$LAUNCHER_DIR/paramrun\n')
    qsubfile.write('echo " "\necho " Job complete at $(date)"\necho " "\n')
    qsubfile.write('finish=$(date +%s)\n')
    qsubfile.write(
        'printf "Job duration: %02d:%02d:%02d (%d s)\n" $(((finish-start)/3600)) $(((finish-start)%3600/60)) $(((finish-start)%60)) $((finish-start))\n')

    qsubfile.close()

    jobid = None
    if not test:
        process = subprocess.Popen('sbatch %s' % qsubfilepath,
                                   shell=True, stdout=subprocess.PIPE,
                                   encoding='utf-8')
        for line in process.stdout:
            print(line.strip())

            if line.find('Submitted batch job') == 0:
                jobid = int(line.strip().split(' ')[3])
        process.wait()

    if not keepqsubfile:
        print('Deleting qsubfile: %s' % qsubfilepath)
        os.remove(qsubfilepath)
    return jobid

if __name__ == "__main__":

    # set up argument parser
    parser = argparse.ArgumentParser(description='process TORQUE job.')
    parser.add_argument('-N', '--nodes', help='minimum number of nodes',
                        dest='nodes', default=None)
    parser.add_argument('-e', '--tasks-per-node',
                        help='number of tasks per node',
                        dest='tpn', default=None)
    parser.add_argument('-s', '--script', help='name of parallel script to run',
                        dest='script_name')
    parser.add_argument('-l', '--runtime', help='maximum runtime for job',
                        default='01:00:00', dest='runtime')
    parser.add_argument('-r', '--memory', help='maximum runtime for job',
                        default='4gb', dest='runtime')
    parser.add_argument('-J', '--jobname', help='job name',
                        default='launch', dest='jobname')
    parser.add_argument('-o', '--outfile', help='output file',
                        default=None, dest='outfile')
    parser.add_argument('-p', '-q', '--queue', help='name of queue',
                        default='normal', dest='queue')
    parser.add_argument('-A', '--projname', help='name of project',
                        dest='projname', default='ANTS')
    parser.add_argument('-m', '--email', help='email address for notification',
                        dest='email')
    parser.add_argument('-D', '--cwd', help='name of working directory',
                        dest='directory')
    parser.add_argument('-f', '--qsubfile', help='name of batch file',
                        dest='qsubfile')
    parser.add_argument('-w', '--waitproc', help='process to wait for',
                        dest='waitproc')
    parser.add_argument('-k', '--keepqsubfile', help='keep qsub file',
                        dest='keepqsubfile', action="store_true", default=False)
    parser.add_argument('-t', '--test', help='do not actually launch job',
                        dest='test', action="store_true", default=False)
    parser.add_argument('-c', '--compiler', help='compiler (default=intel)',
                        dest='compiler', default='intel')
    parser.add_argument('-d', '-i', '--hold_jid',
                        help='wait for this job id to complete before running',
                        dest='hold', default=None)
    parser.add_argument('-b', '--schedule', default='interleaved',
                        help="schedule type (default: interleaved)")

    (args, command) = parser.parse_known_args(sys.argv[1:])

    if len(command) > 0:
        cmd = ' '.join(command)
    else:
        cmd = ''

    launch_torque(cmd=cmd, script_name=args.script_name,
        runtime=args.runtime, mem=args.memory, jobname=args.jobname,
        outfile=args.outfile, projname=args.projname,
        queue=args.queue, email=args.email,
        qsubfile=args.qsubfile, keepqsubfile=args.keepqsubfile,
        test=args.test, compiler=args.compiler, hold=args.hold,
        cwd=args.directory, nnodes=args.nodes,
        tpn=args.tpn, antsproc=args.antsproc,
        schedule=args.schedule)
                   
