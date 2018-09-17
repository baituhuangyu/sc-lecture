#!/usr/bin/env python
# encoding: utf-8


import sys
from fabric.api import *


reload(sys)
sys.setdefaultencoding('utf8')

TAR_FILE = 'update-saic-neo4j.tar.gz'
PROJECT = 'update-saic-neo4j'
REMOTE_PATH = '/home/yu.huang/app/update-saic-neo4j'
DEPLOY_USER = 'yu.huang'
# DEPLOY_USER = 'huangyu'
DEPLOY_USER_GROUP = '%s:%s' % (DEPLOY_USER, DEPLOY_USER)


def pkg_tar():
    with settings(warn_only=True):
        local("rm -f */*.pyc", shell='/bin/bash')
        # rm log files
        local("rm -rf py_log", shell='/bin/bash')
        local("rm -rf data", shell='/bin/bash')
        # tar
        local("tar -zcf {fname} * --exclude-vcs --exclude=*.csv".format(fname=TAR_FILE), shell='/bin/bash')


@parallel(pool_size=10)
def deploy(model, worker_num=1):
    remote_path = REMOTE_PATH + "_" + model
    with settings(warn_only=True):
        run('rm -rf ' + remote_path + "*")
    run('mkdir -p ' + remote_path)
    run('chown -R ' + DEPLOY_USER_GROUP + ' ' + remote_path)
    # put('*', remote_path)
    put(TAR_FILE, remote_path + '/')

    with cd(remote_path):
        run('tar -zxf ' + TAR_FILE, shell='/bin/bash')
        run('rm ' + TAR_FILE, shell='/bin/bash')

    for i in range(worker_num):
        i_path = remote_path + "_" + str(i)
        run('cp -R ' + remote_path + ' ' + i_path)
        run('chown -R ' + DEPLOY_USER_GROUP + ' ' + i_path)
        with cd(i_path):
            run('chmod 777 ./update_saic_neo4j.py')
            # tk_name = 'update_saic_neo4j.py'
            run('python ./update_saic_neo4j.py -m %s 1>/dev/null 2>/dev/null &' % model, pty=False)


@parallel(pool_size=10)
def kill_all():
    with settings(warn_only=True):
        remote_path = REMOTE_PATH
        tk_name = 'update_saic_neo4j.py'
        run('pkill -f "update_saic_neo4j.py"')
        run('rm -rf %s*' % remote_path)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="请输入参数....", add_help=False)
    # parser.add_argument('-p', help='密码', required=True)
    parser.add_argument('-a', help='action: kill, deploy, pkg', required=True)
    parser.add_argument('-m', help='model: dx, quantum.',
                        default='dx', choices=["dx", "quantum"])
    parser.add_argument('--help', action='help')
    args = parser.parse_args()
    env.password = "1qaz2wsx3edc"
    ip_list = ["54.223.132.41"]
    # env.password = "1qaz2wsx"
    # ip_list = ["192.168.31.36"]
    # ip_list = ["192.168.31.43"]
    env.hosts = map(lambda _: DEPLOY_USER+'@' + _, ip_list)

    if args.a == "kill":
        execute(kill_all)
    elif args.a == "deploy":
        execute(pkg_tar)
        execute(deploy, args.m)
    elif args.a == "pkg":
        execute(pkg_tar)
    else:
        print('程序启动参数有误,自动退出, 请重新启动. -h --help 查看帮助')
        exit()






