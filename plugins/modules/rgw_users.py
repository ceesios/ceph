#!/usr/bin/python

# Copyright 2024 Cees Moerkerken <cees@virtu-on.nl>
#
# GNU General Public License v3.0+

from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.common.dict_transformations \
    import recursive_diff, dict_merge
from socket import error as socket_error
# import boto # s3
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
short_description: create rgw users
description:
    - Create Rados GW users

option:
    rgw_address:
        description:
            - Address of the rados gateway
        required: true

requirements: ['rgwadmin', 'boto']

author:
    - 'Cees Moerkerken'

'''

EXAMPLES = r'''

'''

RETURN = r'''
'''

def get_user(rgw, uid, result):
    try:
        user = rgw.get_user(uid=uid)
    except rgwadmin.exceptions.NoSuchUser as e:
        # it doesnt exist
        userout = None

    else:
        userout = {}
        # get_user also returs parameters wich we can't change.
        userout = dict(
            user_id = user["user_id"],
            tenant = user["tenant"],
            display_name = user["display_name"],
            email = user["email"],
            # default_placement = user["default_placement"],
            # generate_key = user["generate_key"],
            # key_type = user["key_type"],
            max_buckets = user["max_buckets"],
            # placement_tags = user["placement_tags"],
            suspended = user["suspended"],
            # user_caps = user["user_caps"]
        )

        for key in userout:
            if userout[key] == "" or userout[key] == "None":
                userout[key] = None

        if len(user["keys"]) > 0:
            userout["access_key"] = user["keys"][0]["access_key"]
            result["access_key"] = userout["access_key"]
            userout["secret_key"] = user["keys"][0]["secret_key"]
            result["secret_key"] = userout["secret_key"]

        # diff all keys, not just predefined keys
        # for key in user:
        #     userout[key] = user[key]

    return userout


def delete_user(rgw, uid, result):
    userout = None
    try:
        userout = rgw.remove_user(uid=uid)

    except rgwadmin.exceptions.RGWAdminException as e:
        result['msg']=str(e.code)
        result['error_messages'].append(e.raw)

    return userout


def create_user(rgw, newuser_params, result):
    newuser = None

    try:
        newuser = rgw.create_user(**newuser_params)

    except rgwadmin.exceptions.KeyExists as e:
        result['msg']="Access_key not unique"
        result['error_messages'].append("Access_key not unique")
        result['error_messages'].append(e.code)

    except rgwadmin.exceptions.RGWAdminException as e:
        result['msg']=str(e.code)
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
        result['msg']=str(e.code)
        result['error_messages'].append(e.code)

    return newuser


def update_user(rgw, newuser_params, result):
    newuser = None

    try:
        newuser = rgw.modify_user(**newuser_params)

    except rgwadmin.exceptions.KeyExists as e:
        result['msg']="Access_key not unique"
        result['error_messages'].append("Access_key not unique")
        result['error_messages'].append(e.code)

    except rgwadmin.exceptions.RGWAdminException as e:
        result['msg']=str(e.code)
        result['error_messages'].append(e.code)

    else:
        result["access_key"] = newuser["keys"][0]["access_key"]
        result["secret_key"] = newuser["keys"][0]["secret_key"]

    return newuser


def get_user_params(params, add_user_id=False):
    # Set UID to include tenant for some functions
    if params['user_tenant'] is None:
        uid = params['user_id']
    else:
        uid = f"{params['user_tenant']}${params['user_id']}"

    newuser_params = dict(
        uid = uid,
        display_name = params["user_display_name"],
        email = params["user_email"],
        # keys = user_keys,
        # default_placement = params["user_default_placement"],
        max_buckets = params["user_max_buckets"],
        # placement_tags = params["user_placement_tags"],
        suspended = params["user_suspended"],
        # user_caps = params["user_user_caps"]
    )

    if add_user_id:
        newuser_params["user_id"] = params["user_id"]
        newuser_params["tenant"] = params["user_tenant"]

    if params["user_access_key"] is not None:
        newuser_params["access_key"] = params["user_access_key"]
        newuser_params["secret_key"] = params["user_secret_key"]

    return newuser_params


def main():
    argument_spec = {}
    argument_spec.update(
        state=dict(type='str', default="present",
                   choices=['present', 'absent']),
        host=dict(type='str', required=True),
        port=dict(type='str', default="8000"),
        is_secure=dict(type='bool', default=True),
        verify_ssl=dict(type='bool', default=True),
        access_key=dict(type='str', no_log=True),
        secret_key=dict(type='str', no_log=True),
        user_id=dict(type='str', required=True),
        user_tenant = dict(type='str', default=None),
        user_display_name=dict(type='str', default=None),
        user_email=dict(type='str', default=None),
        user_access_key=dict(type='str', required=False),
        # user_default_placement=dict(type='str', default=None),
        user_max_buckets = dict(type='int', default="1000"),
        # user_placement_tags=dict(type='str', default=None),
        user_secret_key=dict(type='str', required=False),
        user_suspended = dict(type='int', default="0"),
        # user_caps = dict(type='list', default=[]),
    )

    module = AnsibleModule(
        argument_spec=argument_spec,
        required_if=[],
        supports_check_mode=True,
        mutually_exclusive=[],
        required_together=[
            ('user_access_key', 'user_secret_key')
        ],
        required_one_of=[]
    )

    # seed the result dict in the object
    result = dict(
        changed=False,
        msg=None,
        error_messages=[]
    )

    # radosgw connection
    rgw = rgwadmin.RGWAdmin(
            access_key=module.params.get('access_key'),
            secret_key=module.params.get('secret_key'),
            server=f"{module.params.get('host')}:{module.params.get('port')}",
            secure=module.params.get('is_secure'),
            verify=module.params.get('verify_ssl')
        )

    # test connection
    try:
        rgw.get_usage()
    except rgwadmin.exceptions.ServerDown as e:
        module.fail_json(msg="ServerDown")
    except rgwadmin.exceptions.RGWAdminException as e:
        module.fail_json(msg=str(e.code), error_messages=e.raw)

    # Create newuser_params
    newuser_params = get_user_params(module.params)
    newuser_params_user_id = get_user_params(module.params, add_user_id = True)
    uid = newuser_params_user_id.pop("uid")
    result["uid"] = uid

    # test if user exists
    before_user = get_user(rgw, result["uid"], result)

    # Ignore existing keys when keys is None
    if module.params['user_access_key'] is None:
        if before_user is not None:
            del before_user['access_key']
            del before_user['secret_key']

    # Check if changes are needed
    if module.params['state'] == "present":
        if before_user != newuser_params_user_id:
            result['changed'] = True
            if module._diff:
                result['diff'] = dict(
                    before=before_user,
                    after=newuser_params_user_id
                )
    if module.params['state'] == "absent":
        if before_user is not None:
            result['changed'] = True
            if module._diff:
                result['diff'] = dict(
                    before=before_user,
                    after=None
                )


    # EXIT also in check mode
    if len(result['error_messages']) > 0:
        module.fail_json(msg=result['error_messages'])

    # exit when in check mode
    if module.check_mode:
        module.exit_json(**result)

    if result['changed']:
        if before_user is None and module.params['state'] == "present":
            create_user(rgw, newuser_params, result)
        elif before_user is not None and module.params['state'] == "absent":
            delete_user(rgw, uid, result)
        elif before_user != newuser_params_user_id:
            if before_user["access_key"] != newuser_params_user_id["access_key"]:
                remove_key(rgw, newuser_params, before_user["access_key"], result)
            update_user(rgw, newuser_params, result)

    # EXIT
    if len(result['error_messages']) > 0:
        module.fail_json(msg=result['error_messages'])
    else:
        module.exit_json(**result)



if __name__ == '__main__':
    main()
