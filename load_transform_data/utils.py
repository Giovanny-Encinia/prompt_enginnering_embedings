import os


def get_azure_primary_key(name_env_variable: str = "AZURE_KEY") -> str:
    """
    Retrieve the Azure primary key from an environment variable.

    This function retrieves the Azure primary key from the specified environment
    variable. If the environment variable is not set or has no value, an exception
    is raised with a corresponding error message.

    Parameters
    ----------
    name_env_variable : str, optional
        The name of the environment variable containing the Azure primary key.
        The default is "AZURE_KEY".

    Returns
    -------
    str
        The retrieved Azure primary key.

    Raises
    ------
    Exception
        If the specified environment variable is not set or has no value, an
        exception is raised with an error message suggesting to reload the
        environment or create the variable.
    """
    azure_key = os.getenv(name_env_variable, False)

    if not azure_key:
        raise Exception(
            "There is no variable, reload the environment or create the variable"
        )

    return azure_key


def get_azure_base(name_env_variable: str = "AZURE_BASE") -> str:
    """
    Retrieve the Azure base from an environment variable.

    This function retrieves the Azure base from the specified environment
    variable. If the environment variable is not set or has no value, an exception
    is raised with a corresponding error message.

    Parameters
    ----------
    name_env_variable : str, optional
        The name of the environment variable containing the Azure primary key.
        The default is "AZURE_BASE".

    Returns
    -------
    str
        The retrieved Azure primary key.

    Raises
    ------
    Exception
        If the specified environment variable is not set or has no value, an
        exception is raised with an error message suggesting to reload the
        environment or create the variable.
    """
    azure_base = os.getenv(name_env_variable, False)

    if not azure_base:
        raise Exception(
            "There is no variable, reload the environment or create the variable"
        )

    return azure_base


def get_file_full_path(filename: str) -> str:
    root_path = os.path.abspath(os.path.join(os.getcwd(), "."))
    # Path to the "data" folder in the root path
    data_folder_path = os.path.join(root_path, "data")
    filepath = os.path.join(data_folder_path, filename)
    return filepath
