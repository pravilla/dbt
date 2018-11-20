
from .renderer import ConfigRenderer
from .profile import Profile
from .project import Project
from .profile import read_profile
from .profile import PROFILES_DIR
from .runtime import RuntimeConfig


def read_config(profiles_dir):
    profile = read_profile(profiles_dir)
    if profile is None:
        return {}
    else:
        return profile.get('config', {})


def send_anonymous_usage_stats(config):
    return config.get('send_anonymous_usage_stats', True)


def colorize_output(config):
    return config.get('use_colors', True)


def read_profiles(profiles_dir=None):
    """This is only used in main, for some error handling"""
    if profiles_dir is None:
        profiles_dir = PROFILES_DIR

    raw_profiles = read_profile(profiles_dir)

    if raw_profiles is None:
        profiles = {}
    else:
        profiles = {k: v for (k, v) in raw_profiles.items() if k != 'config'}

    return profiles
