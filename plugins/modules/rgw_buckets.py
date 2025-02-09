import boto3
from botocore.exceptions import ClientError
from ansible.module_utils.basic import AnsibleModule

ANSIBLE_METADATA = {
    'metadata_version': '1.1',
    'status': ['preview'],
    'supported_by': 'community'
}

DOCUMENTATION = '''
---
module: rgw_buckets
short_description: Manage S3 buckets using boto3
description:
    - This module allows you to create and delete S3 buckets using boto3.
version_added: "2.9"
author: "Cees Moerkerken (@ceesios)"
options:
    state:
        description:
            - Whether the bucket should be present or absent.
        required: true
        choices: ['present', 'absent']
    bucket_name:
        description:
            - The name of the S3 bucket.
        required: true
    access_key:
        description:
            - AWS access key.
        required: true
    secret_key:
        description:
            - AWS secret key.
        required: true
    host:
        description:
            - The endpoint host for the S3 service.
        required: false
    port:
        description:
            - The endpoint port for the S3 service.
        required: false
    region:
        description:
            - The AWS region to use.
        required: false
    use_ssl:
        description:
            - Whether to use SSL.
        required: false
        type: bool
        default: true
    verify:
        description:
            - Whether to verify SSL certificates.
        required: false
        type: bool
        default: true
'''

EXAMPLES = '''
# Create an S3 bucket
- name: Create an S3 bucket
  rgw_buckets:
    state: present
    bucket_name: my-test-bucket
    access_key: "YOUR_ACCESS_KEY"
    secret_key: "YOUR_SECRET_KEY"
    host: "s3.your-endpoint.com"
    port: 443
    region: "us-west-1"
    use_ssl: true
    verify: true

# Delete an S3 bucket
- name: Delete an S3 bucket
  rgw_buckets:
    state: absent
    bucket_name: my-test-bucket
    access_key: "YOUR_ACCESS_KEY"
    secret_key: "YOUR_SECRET_KEY"
    host: "s3.your-endpoint.com"
    port: 443
    region: "us-west-1"
    use_ssl: true
    verify: true
'''

RETURN = '''
bucket:
    description: Details about the bucket that was created or deleted.
    type: dict
    returned: when state is present and bucket_name is provided
    sample: {
        "Name": "my-test-bucket",
        "CreationDate": "2021-01-01T00:00:00.000Z"
    }
'''

def create_bucket(s3_client, bucket_name, result):
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        result['changed'] = True
    except ClientError as e:
        result['error_messages'].append(str(e))

def delete_bucket(s3_client, bucket_name, result):
    try:
        s3_client.delete_bucket(Bucket=bucket_name)
        result['changed'] = True
    except ClientError as e:
        result['error_messages'].append(str(e))

def main():
    argument_spec = dict(
        state=dict(type='str', required=True, choices=['present', 'absent']),
        bucket_name=dict(type='str', required=True),
        access_key=dict(type='str', required=True, no_log=True),
        secret_key=dict(type='str', required=True, no_log=True),
        host=dict(type='str', required=False),
        port=dict(type='int', required=False),
        region=dict(type='str', required=False),
        use_ssl=dict(type='bool', required=False, default=True),
        verify=dict(type='bool', required=False, default=True)
    )

    result = dict(
        changed=False,
        error_messages=[]
    )

    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=True,
        required_together=[['host', 'port']]
    )

    endpoint_url = None
    if module.params['host'] and module.params['port']:
        protocol = 'https' if module.params['use_ssl'] else 'http'
        endpoint_url = f"{protocol}://{module.params['host']}:{module.params['port']}"

    s3_client = boto3.client(
        's3',
        aws_access_key_id=module.params['access_key'],
        aws_secret_access_key=module.params['secret_key'],
        endpoint_url=endpoint_url,
        region_name=module.params['region'],
        use_ssl=module.params['use_ssl'],
        verify=module.params['verify']
    )

    if module.params['state'] == 'present':
            create_bucket(s3_client, module.params['bucket_name'], result)
            create_bucket(s3_client, module.params['bucket_name'], result)
        if module.params['policy_name'] and module.params['policy_document']:
            create_policy(iam_client, module.params['policy_name'], module.params['policy_document'], result)
        create_bucket(s3_client, module.params['bucket_name'], result)
        if module.params['policy_name'] and module.params['policy_document']:
            create_policy(iam_client, module.params['policy_name'], module.params['policy_document'], result)
    elif module.params['state'] == 'absent':
            delete_bucket(s3_client, module.params['bucket_name'], result)
            delete_bucket(s3_client, module.params['bucket_name'], result)
        if module.params['policy_arn']:
            delete_policy(iam_client, module.params['policy_arn'], result)
        delete_bucket(s3_client, module.params['bucket_name'], result)
        if module.params['policy_arn']:
            delete_policy(iam_client, module.params['policy_arn'], result)

    if len(result['error_messages']) > 0:
        module.fail_json(msg=result['error_messages'])
    else:
        module.exit_json(**result)

if __name__ == '__main__':
    main()