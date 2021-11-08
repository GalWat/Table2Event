from dynaconf import Dynaconf, Validator

settings = Dynaconf(
    envvar_prefix="DYNACONF",
    settings_files=['settings.yaml', '.secrets.yaml'],

    validators=[
        Validator(
            'used_dbms', 'dbms_host', 'database', 'users_table', 'user_id_column',
            'dbms_secrets.login', 'dbms_secrets.password',
            must_exist=True
        ),
        Validator(
            'used_dbms', is_in=['mysql']
        ),
        Validator(
            'endpoint_api', is_in=['amplitude']
        )
    ]
)
