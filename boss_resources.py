import math

from requests import HTTPError

from intern.remote.boss import BossRemote
from intern.resource.boss.resource import *


def get_boss_project(rmt, proj_setup):
    try:
        proj_actual = rmt.get_project(proj_setup)
    except HTTPError:
        try:
            proj_actual = rmt.create_project(proj_setup)
        except Exception as e:
            print(type(e), e)
            raise(e)
    except Exception as e:
        print(type(e), e)
        raise(e)
    return proj_actual


def setup_boss_collection(rmt, coll_name):
    coll_setup = CollectionResource(coll_name)
    return get_boss_project(rmt, coll_setup)


def setup_boss_coord_frame(rmt, coord_frame_name, image_size, voxel_size, voxel_unit):
    coord_setup = CoordinateFrameResource(coord_frame_name, '', 0,
                                          image_size[0], 0, image_size[1], 0, image_size[2],
                                          voxel_size[0], voxel_size[1], voxel_size[2], voxel_unit)
    return get_boss_project(rmt, coord_setup)


def create_hierarchy_levels(image_size, lowest_res=64):
    max_xy = max(image_size[0:1])
    return math.ceil(math.log(max_xy / lowest_res, 2))


def setup_boss_experiment(rmt, exp_name, coll_name, coord_frame_name, image_size, voxel_size):
    # if all elements are the same, isotropic, otherwise anisotropic
    if (len(set(voxel_size)) <= 1):
        hierarchy_method = 'isotropic'
    else:
        hierarchy_method = 'anisotropic'

    num_hierarchy_levels = create_hierarchy_levels(image_size)

    exp_setup = ExperimentResource(exp_name, coll_name, coord_frame_name, '',
                                   num_hierarchy_levels, hierarchy_method)
    return get_boss_project(rmt, exp_setup)


def setup_boss_channel(rmt, ch_name, coll_name, exp_name, datatype, res, ch_type='image', ch_description=''):
    ch_setup = ChannelResource(
        ch_name, coll_name, exp_name, ch_type, ch_description, 0, datatype, res)
    return get_boss_project(rmt, ch_setup)


def setup_boss_resources(rmt, coll_name, exp_name, ch_name, voxel_size, voxel_unit, datatype, res, image_size):
    coord_frame_name = coll_name + '_' + exp_name
    coll = setup_boss_collection(rmt, coll_name)
    coord_frame = setup_boss_coord_frame(
        rmt, coord_frame_name, image_size, voxel_size, voxel_unit)
    experiment = setup_boss_experiment(
        rmt, exp_name, coll_name, coord_frame_name, image_size, voxel_size)
    channel = setup_boss_channel(
        rmt, ch_name, coll_name, exp_name, datatype, res)
    return (coll, coord_frame, experiment, channel)
