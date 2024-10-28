#!/usr/bin/python

# Copyright 2024 Cees Moerkerken <cees@virtu-on.nl>
#
# GNU General Public License v3.0+

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.common.dict_transformations \
    import recursive_diff, dict_merge
from socket import error as socket_error
# import boto # s3
import radosgw

ANSIBLE_METADATA = {
    'metadata_version': '1.0',
    'status': ['preview'],
    'supported_by': 'community'
}

DOCUMENTATION = r'''
---
module: rgw_users
short_description: create rgw users
description:
    - Create Rados GW users

option:
    rgw_address:
        description:
            - Address of the rados gateway
        required: true

requirements: ['radosgw', 'boto']

author:
    - 'Cees Moerkerken'

'''

EXAMPLES = r'''

'''

RETURN = r'''
'''

def get_user(rgw, uid):
    try:
        user = rgw.get_user(uid=uid)

        userout = dict(
            # caps = user.caps,
            display_name = user.display_name,
            email = user.email,
            keys = str(user.keys),
            max_buckets = user.max_buckets,
            # subusers = str(user.subusers),
            # suspended = user.suspended,
            # swift_keys = str(user.swift_keys),
            tenant = user.tenant,
            user_id = user.user_id
        )
    except radosgw.exception.NoSuchUser:
        # it doesnt exist
        userout = None
    return userout


def delete_user(rgw, uid, result):
    userout = None
    try:
        userout = rgw.delete_user(uid=uid)

    except radosgw.exception.RadosGWAdminError as e:
        result['error_messages'].append(e.get_code())
        result['error_messages'].append(uid)
    except radosgw.exception.NoSuchUser:
        userout = None
    return userout


def create_user(rgw, uid, user, result):
    userout = None
    newuser_params = dict(
        uid = uid,
        display_name = user['display_name'],
        email = user['email'],
        # key_type = user.key_type, #  the key_type 's3' or 'swift'. Default: 's3'
        # access_key = user.access_key,
        # secret_key = user.secret_key,
        # generate_key = user.generate_key, # True to auto generate a new key pair. Default: True
        # user_caps = user.user_caps,
        max_buckets = user['max_buckets']
        # suspended = user.suspended
    )

    try:
        newuser = rgw.create_user(**newuser_params)

        userout = dict(
            keys = str(newuser.keys),
            newuser_id = newuser.user_id
        )
    except radosgw.exception.RadosGWAdminError as e:
        result['error_messages'].append(e.get_code())

    return userout


def update_user(rgw, uid, user, result):
    userout = None
    try:
        newuser = rgw.update_user(
                uid = uid,
                display_name = user['display_name'],
                # email = user['email'],
                # key_type = user.key_type, #  the key_type 's3' or 'swift'. Default: 's3'
                # access_key = user.access_key,
                # secret_key = user.secret_key,
                # generate_key = user.generate_key, # True to auto generate a new key pair. Default: True
                # user_caps = user.user_caps,
                # max_buckets = user['max_buckets']
                # suspended = user.suspended
            )

        userout = dict(
            keys = str(newuser.keys),
            newuser_id = newuser.user_id
        )
    except radosgw.exception.RadosGWAdminError as e:
        result['error_messages'].append(str(e))

    return userout


def main():
    argument_spec = {}
    argument_spec.update(
        state=dict(type='str', default="present",
                   choices=['present', 'absent']),
        host=dict(type='str', required=True),
        port=dict(type='str', default="8000"),
        is_secure=dict(type='bool', default=True),
        access_key=dict(type='str', no_log=True),
        secret_key=dict(type='str', no_log=True),
        user=dict(
            type='dict',
            required=True,
            options=dict(
                # caps = dict(type='list', default=[]),
                display_name=dict(type='str', default=""),
                email=dict(type='str', default=""),
                keys = dict(type='str', required=False),
                max_buckets = dict(type='int', default="1000"),
                # subusers = dict(type='list', default=[]),
                # suspended = dict(type='int', default="0"),
                # swift_keys = dict(type='str', default="[]"),
                tenant = dict(type='str', default=None),
                user_id=dict(type='str', required=True),
            )
        )
    )

    module = AnsibleModule(
        argument_spec=argument_spec,
        required_if=[],
        supports_check_mode=True,
        mutually_exclusive=[],
        required_together=[],
        required_one_of=[]
    )

    # seed the result dict in the object
    result = dict(
        changed=False,
        msg=None,
        error_messages=[]
    )

    # radosgw connection
    rgw = radosgw.connection.RadosGWAdminConnection(
            host=module.params.get('host'),
            port=module.params.get('port'),
            is_secure=module.params.get('is_secure'),
            access_key=module.params.get('access_key'),
            secret_key=module.params.get('secret_key'),
            aws_signature='AWS4'
        )

    # test connection
    try:
        rgw.get_usage()
    except radosgw.exception.RadosGWAdminError as e:
        module.fail_json(msg=e.get_code(), **result)
    except socket_error as e:
        module.fail_json(msg=str(e), **result)

    # Set UID to include tenant for some functions
    if module.params['user']['tenant'] is None:
        uid = module.params['user']['user_id']
    else:
        uid = f"{module.params['user']['tenant']}${module.params['user']['user_id']}"

    # test if user exists
    before_user = get_user(rgw, uid)

    # Ignore existing keys when not defined
    if before_user is not None:
        if module.params['user']['keys'] is None:
            result['user_keys'] = before_user.pop("keys")
            module.params['user'].pop("keys")

    # Check if changes are needed
    if module.params['state'] == "present":
        if before_user != module.params['user']:
            result['changed'] = True
            if module._diff:
                result['diff'] = dict(
                    before=before_user,
                    after=module.params['user']
                )
    if module.params['state'] == "absent":
        if before_user is not None:
            result['changed'] = True
            if module._diff:
                result['diff'] = dict(
                    before=before_user,
                    after=None
                )


    # exit when in check mode
    if module.check_mode:
        module.exit_json(**result)

    if result['changed']:
        if before_user is None and module.params['state'] == "present":
            create_user(rgw, uid, module.params['user'], result)
        elif before_user is not None and module.params['state'] == "absent":
            delete_user(rgw, uid, result)
        elif before_user != module.params['user']:
            update_user(rgw, uid, module.params['user'], result)

    # EXIT
    if len(result['error_messages']) > 0:
        module.fail_json(msg=result['error_messages'])
    else:
        module.exit_json(**result)



if __name__ == '__main__':
    main()
