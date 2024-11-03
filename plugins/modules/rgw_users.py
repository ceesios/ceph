#!/usr/bin/python

# Copyright 2024 Cees Moerkerken <cees@virtu-on.nl>
#
# GNU General Public License v3.0+

from ansible.module_utils.basic import AnsibleModule
from socket import error as socket_error
import rgwadmin
import rgwadmin.exceptions

ANSIBLE_METADATA = {
    'metadata_version': '1.0',
    'status': ['preview'],
    'supported_by': 'community'
}

DOCUMENTATION = r'''
---
module: rgw_users
short_description: Manage RGW users using rgwadmin
description:
    - This module allows you to create and delete RGW users using rgwadmin.
version_added: "2.9"
author: "Cees Moerkerken (@ceesmoerkerken)"
options:
    state:
        description:
            - Whether the user should be present or absent.
        required: false
        choices: ['present', 'absent']
        default: present
    host:
        description:
            - The endpoint host for the RGW service.
        required: true
    port:
        description:
            - The endpoint port for the RGW service.
        required: true
    is_secure:
        description:
            - Whether to use SSL.
        required: false
        type: bool
        default: true
    verify_ssl:
        description:
            - Whether to verify SSL certificates.
        required: false
        type: bool
        default: true
    access_key:
        description:
            - AWS access key.
        required: true
        no_log: true
    secret_key:
        description:
            - AWS secret key.
        required: true
        no_log: true
    user_id:
        description:
            - The ID of the user.
        required: true
    user_tenant:
        description:
            - The tenant of the user.
        required: false
        default: None
    user_display_name:
        description:
            - The display name of the user.
        required: false
        default: None
    user_email:
        description:
            - The email of the user.
        required: false
        default: None
    user_access_key:
        description:
            - The access key of the user.
        required: false
    user_secret_key:
        description:
            - The secret key of the user.
        required: false
    user_max_buckets:
        description:
            - The maximum number of buckets the user can create.
        required: false
        default: 1000
    user_suspended:
        description:
            - Whether the user is suspended.
        required: false
        default: 0
    admin_caps:
        description:
            - The admin capabilities of the user.
        required: false
        default: None
requirements:
    - rgwadmin
'''

EXAMPLES = r'''
# Create an RGW user
- name: Create an RGW user
  rgw_users:
    state: present
    host: "rgw.your-endpoint.com"
    port: 8000
    is_secure: true
    verify_ssl: true
    access_key: "YOUR_ACCESS_KEY"
    secret_key: "YOUR_SECRET_KEY"
    user_id: "my-test-user"
    user_display_name: "Test User"
    user_email: "testuser@example.com"
    user_max_buckets: 1000
    user_suspended: 0
    admin_caps: "buckets=*"

# Delete an RGW user
- name: Delete an RGW user
  rgw_users:
    state: absent
    host: "rgw.your-endpoint.com"
    port: 8000
    is_secure: true
    verify_ssl: true
    access_key: "YOUR_ACCESS_KEY"
    secret_key: "YOUR_SECRET_KEY"
    user_id: "my-test-user"
'''

RETURN = r'''
user:
    description: Details about the user that was created or deleted.
    type: dict
    returned: when state is present and user_id is provided
    sample: {
        "user_id": "my-test-user",
        "display_name": "Test User",
        "email": "testuser@example.com",
        "access_key": "YOUR_ACCESS_KEY",
        "secret_key": "YOUR_SECRET_KEY"
    }
'''

def get_user(rgw, uid, result):
    try:
        user = rgw.get_user(uid=uid)
    except rgwadmin.exceptions.NoSuchUser as e:
        userout = None
    else:
        userout = {
            "user_id": user["user_id"],
            "tenant": user["tenant"],
            "display_name": user["display_name"],
            "email": user["email"],
            "max_buckets": user["max_buckets"],
            "suspended": user["suspended"]
        }

        for key in userout:
            if userout[key] == "" or userout[key] == "None" or userout[key] == []:
                userout[key] = None

        if len(user["keys"]) > 0:
            userout["access_key"] = user["keys"][0]["access_key"]
            result["access_key"] = userout["access_key"]
            userout["secret_key"] = user["keys"][0]["secret_key"]
            result["secret_key"] = userout["secret_key"]

    return userout


def delete_user(rgw, uid, result):
    userout = None
    try:
        userout = rgw.remove_user(uid=uid)
    except rgwadmin.exceptions.RGWAdminException as e:
        result['msg'] = str(e.code)
        result['error_messages'].append(e.code)
    return userout


def create_user(rgw, newuser_params, result):
    newuser = None
    try:
        newuser = rgw.create_user(**newuser_params)
    except rgwadmin.exceptions.KeyExists as e:
        result['msg'] = "Access_key not unique"
        result['error_messages'].append("Access_key not unique")
        result['error_messages'].append(e.code)
    except rgwadmin.exceptions.RGWAdminException as e:
        result['msg'] = str(e.code)
        result['error_messages'].append(e.code)
    else:
        result["access_key"] = newuser["keys"][0]["access_key"]
        result["secret_key"] = newuser["keys"][0]["secret_key"]
    return newuser


def remove_key(rgw, newuser_params, old_access_key, result):
    newuser = None
    try:
        newuser = rgw.remove_key(access_key=old_access_key, uid=newuser_params["uid"])
    except rgwadmin.exceptions.RGWAdminException as e:
        result['msg'] = str(e.code)
        result['error_messages'].append(e.code)
    return newuser


def update_user(rgw, newuser_params, result):
    newuser = None
    try:
        newuser = rgw.modify_user(**newuser_params)
    except rgwadmin.exceptions.KeyExists as e:
        result['msg'] = "Access_key not unique"
        result['error_messages'].append("Access_key not unique")
        result['error_messages'].append(e.code)
    except rgwadmin.exceptions.RGWAdminException as e:
        result['msg'] = str(e.code)
        result['error_messages'].append(e.code)
    else:
        result["access_key"] = newuser["keys"][0]["access_key"]
        result["secret_key"] = newuser["keys"][0]["secret_key"]
    return newuser


def get_user_params(params, add_user_id=False):
    if params['user_tenant'] is None:
        uid = params['user_id']
    else:
        uid = f"{params['user_tenant']}${params['user_id']}"

    newuser_params = {
        "uid": uid,
        "display_name": params["user_display_name"],
        "email": params["user_email"],
        "max_buckets": params["user_max_buckets"],
        "suspended": params["user_suspended"],
        "user_caps": params["admin_caps"]
    }

    if add_user_id:
        newuser_params["user_id"] = params["user_id"]
        newuser_params["tenant"] = params["user_tenant"]

    if params["user_access_key"] is not None:
        newuser_params["access_key"] = params["user_access_key"]
        newuser_params["secret_key"] = params["user_secret_key"]

    return newuser_params


def main():
    argument_spec = {
        "state": dict(type='str', default="present", choices=['present', 'absent']),
        "host": dict(type='str', required=True),
        "port": dict(type='str', default="8000"),
        "is_secure": dict(type='bool', default=True),
        "verify_ssl": dict(type='bool', default=True),
        "access_key": dict(type='str', required=True, no_log=True),
        "secret_key": dict(type='str', required=True, no_log=True),
        "user_id": dict(type='str', required=True),
        "user_tenant": dict(type='str', default=None),
        "user_display_name": dict(type='str', default=None),
        "user_email": dict(type='str', default=None),
        "user_access_key": dict(type='str', required=False),
        "user_secret_key": dict(type='str', required=False),
        "user_max_buckets": dict(type='int', default=1000),
        "user_suspended": dict(type='int', default=0),
        "admin_caps": dict(type='str', default=None)
    }

    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=True,
        required_together=[('user_access_key', 'user_secret_key')]
    )

    result = {
        "changed": False,
        "msg": None,
        "error_messages": []
    }

    rgw = rgwadmin.RGWAdmin(
        access_key=module.params.get('access_key'),
        secret_key=module.params.get('secret_key'),
        server=f"{module.params.get('host')}:{module.params.get('port')}",
        secure=module.params.get('is_secure'),
        verify=module.params.get('verify_ssl')
    )

    try:
        rgw.get_usage()
    except rgwadmin.exceptions.ServerDown as e:
        module.fail_json(msg="ServerDown")
    except rgwadmin.exceptions.RGWAdminException as e:
        module.fail_json(msg=str(e.code), error_messages=e.raw)

    newuser_params = get_user_params(module.params)
    newuser_params_user_id = get_user_params(module.params, add_user_id=True)
    newuser_params_wo_caps = newuser_params_user_id.copy()
    newuser_params_wo_caps.pop("user_caps")
    uid = newuser_params_user_id.pop("uid")
    result["uid"] = uid

    before_user = get_user(rgw, result["uid"], result)

    if module.params['user_access_key'] is None:
        if before_user is not None:
            del before_user['access_key']
            del before_user['secret_key']

    if module.params['state'] == "present":
        if before_user != newuser_params_wo_caps:
            result['changed'] = True
            if module._diff:
                result['diff'] = {
                    "before": before_user,
                    "after": newuser_params_wo_caps
                }
    if module.params['state'] == "absent":
        if before_user is not None:
            result['changed'] = True
            if module._diff:
                result['diff'] = {
                    "before": before_user,
                    "after": None
                }

    if len(result['error_messages']) > 0:
        module.fail_json(msg=result['error_messages'])

    if module.check_mode:
        module.exit_json(**result)

    if result['changed']:
        if before_user is None and module.params['state'] == "present":
            create_user(rgw, newuser_params, result)
        elif before_user is not None and module.params['state'] == "absent":
            delete_user(rgw, uid, result)
        elif before_user != newuser_params_user_id:
            if "access_key" in newuser_params_user_id and before_user["access_key"] != newuser_params_user_id["access_key"]:
                remove_key(rgw, newuser_params, before_user["access_key"], result)
            update_user(rgw, newuser_params, result)

    if len(result['error_messages']) > 0:
        module.fail_json(msg=result['error_messages'])
    else:
        module.exit_json(**result)


if __name__ == '__main__':
    main()