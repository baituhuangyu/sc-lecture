#!/usr/bin/env python
# coding: utf-8

import os
import boto3
from boto3.exceptions import *
import ConfigParser
from config.config import AwsConf

try:
    cp = ConfigParser.ConfigParser()
    print os.environ['HOME']
    cp.read(os.environ['HOME'] + '/.aws/credentials')
    access_key = cp.get('default', 'aws_access_key_id')
    secret_key = cp.get('default', 'aws_secret_access_key')
except Exception, e:
    access_key = AwsConf.aws_access_key_id
    secret_key = AwsConf.aws_secret_access_key

boto3.setup_default_session(
    region_name=AwsConf.aws_region_name,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)


class EC2(object):
    def __init__(self):
        self._ec2 = boto3.resource('ec2')
        self._client = boto3.client('ec2')

    def create_instances(self, instances_name, image_id, max_count=1, instance_type='t2.micro', user_data=''):
        """
        创建实例
        :param instances_name: 实例名称
        :param image_id: 根据镜像来创建实例
        :param max_count: 创建实例的最大数量
        :param instance_type: 实例类型,默认 t2.micro
        :param user_data: 初始化实例的时候运行的脚本
        :return: list,创建的实例信息
        """
        ec2 = self._ec2
        client = self._client
        waiter = client.get_waiter('instance_running')

        instances = ec2.create_instances(ImageId=image_id, MinCount=1, MaxCount=max_count,
                                         InstanceType=instance_type,
                                         SecurityGroupIds=['sg-9281b8f7'], KeyName='sc', UserData=user_data)

        waiter.wait(InstanceIds=[item._id for item in instances])
        for instance in instances:
            instance.create_tags(Tags=[{
                'Key': 'Name',
                'Value': instances_name
            }])

        return instances

    def terminate_instances(self, by_tag_name=None, by_ids=None):
        """
        终止实例,两个参数仅需其中一个
        :param by_tag_name: 通过tag:Name终止实例
        :param by_ids: 通过实例id终止实例
        """
        if by_tag_name and by_ids:
            raise Exception('only need one of [by_tag_name, by_ids]')

        client = self._client
        if by_tag_name:
            result = client.describe_tags(Filters=[{
                'Name': 'tag:Name',
                'Values': by_tag_name if isinstance(by_tag_name, list) else [by_tag_name]
            }])
            instances = [__ for __ in map(lambda _: _.get('ResourceId'), Utils.search_dict(result, 'Tags')) if __] if result else []
            if instances:
                client.terminate_instances(InstanceIds=instances)
            else:
                raise ResourceNotExistsError
        elif by_ids:
            instances = by_ids if isinstance(by_ids, list) else [by_ids]
            client.terminate_instances(InstanceIds=instances)
        else:
            raise Exception('required 2 arguments')

    def start_instances(self, instances_id):
        """
        启动一个或多个实例(开机)
        :param instances_id: type(list),实例id
        :return: type(list),实例信息
        """
        if instances_id:
            client = self._client

            result = client.start_instances(
                InstanceIds=instances_id if isinstance(instances_id, list) else [instances_id]
            )

            return result.get('StartingInstances', [])

    def stop_instances(self, instances_id):
        """
        关闭一个或多个实例(关机)
        :param instances_id: type(list),实例编号
        :return: type(list), 实例信息
        """
        if instances_id:
            client = self._client

            result = client.stop_instances(
                InstanceIds=instances_id if isinstance(instances_id, list) else [instances_id],
                Force=True
            )

            return result.get('StoppingInstances', [])

    def get_instances_info(self, by_tag_name=None, by_ids=None):
        """
        获取一个或多个实例信息
        :param by_tag_name: 通过tag:Name获取
        :param by_ids: 通过实例id获取信息
        :return: 实例信息列表
        """
        if by_tag_name and by_ids:
            raise Exception('only need one of [by_tag_name, by_ids]')

        client = self._client
        if by_tag_name:
            result = client.describe_tags(Filters=[{
                'Name': 'tag:Name',
                'Values': by_tag_name if isinstance(by_tag_name, list) else [by_tag_name]
            }])
            instances = Utils.search_dict(result, 'Tags')
        else:
            result = client.describe_instances(
                InstanceIds=by_ids if isinstance(by_ids, list) else [by_ids]
            )
            instances = Utils.search_dict(result, 'Instances')

        return instances

    def list_images(self, image_ids=None):
        """
        列出一个或者所有镜像
        :param image_ids: 镜像编号
        :return: 镜像列表
        """
        client = self._client
        if image_ids:
            images = image_ids if isinstance(image_ids, list) else [image_ids]
            res = client.describe_images(ImageIds=images, Owners=['277371406937'])
        else:
            res = client.describe_images(Owners=['277371406937'])
        return res.get('Images', [])

    def list_tags(self):
        """
        列出所有的标签
        :return: 所有标签
        """
        client = self._client
        res = client.describe_tags()
        return res.get('Tags', [])

    def list_instances(self, by_ids=None, by_tag_name=None):
        """
        列出所有实例
        :param by_ids: 通过实例编号过滤
        :param by_tag_name: 通过标签名过滤
        :return: 所有实例信息
        """
        client = self._client

        ids = by_ids if isinstance(by_ids, list) else [by_ids]
        tags = by_tag_name if isinstance(by_tag_name, list) else [by_tag_name]
        if by_ids and by_tag_name:
            res = client.describe_instances(
                InstanceIds=ids,
                Filters=[
                    {
                        'Name': 'tag:Name',
                        'Values': tags
                    }
                ]
            )
        elif by_ids:
            res = client.describe_instances(InstanceIds=ids)
        elif by_tag_name:
            res = client.describe_instances(Filters=[
                {
                    'Name': 'tag:Name',
                    'Values': tags
                }
            ])
        else:
            res = client.describe_instances()

        return Utils.search_dict(res, 'Instances')

    def change_ip(self, source_public_ip=None, source_id=None, source_tag_name=None, source_private_ip=None):
        """
        换ip,通过给一台实例分配一个elastic ip,再分离实例与elastic ip达到更换ip的目的
        :param source_public_ip: 需要更换ip的实例的公网ip
        :param source_id: 需要更换ip的实例的id
        :param source_tag_name: 需要更换ip的实例的tag:Name的值
        :param source_private_ip: 需要更换ip的实例的私有ip
        :return: None
        """
        client = self._client
        elastic_ip = client.describe_addresses().get('Addresses', [])[0].get('PublicIp', '')
        if source_public_ip:
            filter = [
                {
                    'Name': 'ip-address',
                    'Values': [source_public_ip]
                }
            ]
            instance_id = self.get_instance_id(filter)
        elif source_tag_name:
            filter = [
                {
                    'Name': 'tag:Name',
                    'Values': [source_tag_name]
                }
            ]
            instance_id = self.get_instance_id(filter)
        elif source_private_ip:
            filter = [
                {
                    'Name': 'private-ip-address',
                    'Values': [source_private_ip]
                }
            ]
            instance_id = self.get_instance_id(filter)

        elif source_id:
            instance_id = source_id
        else:
            raise Exception(
                'need argument one or more of [source_public_ip,souece_id,source_tag_name,source_private_ip].')

        print instance_id

        client.associate_address(
            InstanceId=instance_id,
            PublicIp=elastic_ip
        )
        client.disassociate_address(
            PublicIp=elastic_ip
        )

    def get_instance_id(self, instances_filters):
        """
        获取实例的id
        get_instance_id([
            {
                'Name':'public-ip',
                'Values':[
                    '54.222.192.102'
                ]
            }
        ])
        :param instances_filters: type(list), 获取目的实例编号的条件
        :return: 实例的编号
        """
        client = self._client
        res = client.describe_instances(Filters=instances_filters)
        return Utils.search_dict(res, 'InstanceId')


class S3(object):
    def __init__(self):
        self._s3 = boto3.resource('s3')
        self._client = boto3.client('s3')

    def create_bucket(self, bucket_name):
        """
        创建一个s3仓库
        :param bucket_name: 仓库名称
        :return: 仓库链接
        """
        client = self._client
        waiter = client.get_waiter('bucket_not_exists')
        waiter.wait(Bucket=bucket_name)
        res = client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': 'cn-north-1'
            }
        )

        waiter = client.get_waiter('bucket_exists')
        waiter.wait(Bucket=bucket_name)
        return res.get('Location', '')

    def delete_bucket(self, bucket_name):
        """
        删除一个仓库
        :param bucket_name: 仓库名称
        :return: None
        """
        client = self._client
        waiter = client.get_waiter('bucket_exists')
        waiter.wait(Bucket=bucket_name)
        client.delete_bucket(
            Bucket=bucket_name
        )
        waiter = client.get_waiter('bucket_not_exists')
        waiter.wait(Bucket=bucket_name)

    def download_file(self, bucket_name, file_key, file_name):
        """
        下载文件到当前文件夹
        :param bucket_name: 仓库名称
        :param file_key: 文件key(path+filename)
        :param file_name: 文件名
        :return: None
        """
        s3 = self._s3
        client = self._client
        waiter = client.get_waiter('object_exists')
        waiter.wait(Bucket=bucket_name, Key=file_key)
        s3.meta.client.download_file(
            Bucket=bucket_name,
            Key=file_key,
            Filename=file_name
        )

    def list_files(self, bucket_name):
        """
        列出所有文件
        :param bucket_name: 仓库名称
        :return: 所有文件信息
        """
        client = self._client
        res = client.list_objects(
            Bucket=bucket_name
        )
        return res.get('Contents', [])

    def upload_file(self, bucket_name, file_key, file_content):
        """
        上传一个文件
        :param bucket_name: 仓库名称
        :param file_key: 文件key
        :param file_content: 文件内容
        :return: 上传成功:True, 上传失败:False
        """
        client = self._client

        res = client.put_object(
            Body=file_content,
            Bucket=bucket_name,
            Key=file_key
        )
        waiter = client.get_waiter('object_exists')
        waiter.wait(Bucket=bucket_name, Key=file_key)
        if res.get('ResponseMetadata', {}).get('HTTPStatusCode', 0) == 200:
            return True
        else:
            return False

    def delete_file(self, bucket_name, file_key, version_id=None):
        """
        删除一个文件
        :param bucket_name: 仓库名称
        :param file_key: 文件key
        :param version_id: 文件版本号(如果有)
        :return: 删除成功:True, 删除失败:False
        """
        client = self._client
        if version_id:
            res = client.delete_object(
                Bucket=bucket_name,
                Key=file_key,
                VersionId=version_id
            )
        else:
            res = client.delete_object(
                Bucket=bucket_name,
                Key=file_key
            )
        waiter = client.get_waiter('object_not_exists')
        waiter.wait(Bucket=bucket_name, Key=file_key)
        if res.get('ResponseMetadata', {}).get('HTTPStatusCode', 0) == 204:
            return True
        else:
            return False


class RDS(object):
    pass


class Utils(object):
    @classmethod
    def search_dict(cls, dictionary, search_key):
        """
        递归查询字典中指定键的值
        :param dictionary: 目标字典
        :param search_key: 目标键
        :return: 第一个匹配到键的值
        """
        if isinstance(dictionary, list):
            if len(dictionary) == 0:
                return
            for item in dictionary:
                return cls.search_dict(item, search_key)

        if isinstance(dictionary, dict):
            if dictionary.has_key(search_key):
                return dictionary.get(search_key, '')
            else:
                for key, value in dictionary.items():
                    if isinstance(value, list) or isinstance(value, dict):
                        if cls.search_dict(value, search_key):
                            return cls.search_dict(value, search_key)
                        else:
                            continue
                    else:
                        continue
        return


class SQS(object):
    def __init__(self):
        self._sqs = boto3.resource('sqs')
        self._client = boto3.client('sqs')

    def create_queue(self, queue_name):
        try:
            if not self.get_queue_url(queue_name):
                raise Exception('queue already exist: %s' % queue_name)
        except Exception, e:
            pass
        res = self._client.create_queue(QueueName=queue_name)
        return res.get('QueueUrl', '')

    def delete_queue(self, queue_name=None, queue_url=None):
        if not queue_name and not queue_url:
            raise Exception('need arguments [queue_url,queue_name]')
        self._client.delete_queue(
            QueueUrl=queue_url if queue_url else self.get_queue_url(queue_name)
        )

    def delete_message(self, receipt_handle, queue_url=None, queue_name=None):
        if not queue_name and not queue_url:
            raise Exception('need arguments [queue_url,queue_name]')
        self._client.delete_message(
            QueueUrl=queue_url if queue_url else self.get_queue_url(queue_name),
            ReceiptHandle=receipt_handle
        )

    def send_message(self, message_body, queue_url=None, queue_name=None):
        if not queue_name and not queue_url:
            raise Exception('need arguments [queue_url,queue_name]')
        if isinstance(message_body, dict):
            import json
            message_body = json.dumps(message_body, ensure_ascii=False)
        res = self._client.send_message(
            QueueUrl=queue_url if queue_url else self.get_queue_url(queue_name),
            MessageBody=message_body
        )
        print res

    def receive_message(self, queue_url=None, queue_name=None):
        if not queue_name and not queue_url:
            raise Exception('need arguments [queue_url,queue_name]')
        res = self._client.receive_message(
            QueueUrl=queue_url if queue_url else self.get_queue_url(queue_name)
        )

        if not Utils.search_dict(res, 'ReceiptHandle'):
            return
        self.delete_message(Utils.search_dict(res, 'ReceiptHandle'), queue_url, queue_name)
        return res.get('Messages', [])

    def get_queue_url(self, queue_name):
        res = self._client.get_queue_url(QueueName=queue_name, QueueOwnerAWSAccountId='277371406937')
        return res.get('QueueUrl', '')

    def purge_queue(self, queue_url=None, queue_name=None):
        if not queue_name and not queue_url:
            raise Exception('need arguments [queue_url,queue_name]')
        self._client.purge_queue(
            QueueUrl=queue_url if queue_url else self.get_queue_url(queue_name)
        )


if __name__ == '__main__':
    pass
