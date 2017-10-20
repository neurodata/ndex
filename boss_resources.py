import math

from requests import HTTPError

from intern.remote.boss import BossRemote
from intern.resource.boss.resource import *


class BossResParams():
    def __init__(self, coll_name, exp_name, ch_name, voxel_size=None, voxel_unit=None, datatype=None, res=None, img_size=None):
        coord_frame_name = coll_name + '_' + exp_name

        self.coll_name = coll_name
        self.coord_frame_name = coord_frame_name
        self.exp_name = exp_name
        self.ch_name = ch_name
        self.voxel_size = voxel_size
        self.voxel_unit = voxel_unit
        self.datatype = datatype
        self.res = res
        self.img_size = img_size

    def setup_resources(self, rmt, get_only=True):
        self.rmt = rmt

        self.coll_resource = self.setup_boss_collection(get_only=get_only)
        self.coord_frame_resource = self.setup_boss_coord_frame(
            get_only=get_only)
        self.exp_resource = self.setup_boss_experiment(get_only=get_only)
        self.ch_resource = self.setup_boss_channel(get_only=get_only)

        if get_only:
            self.voxel_size = [self.coord_frame_resource.x_voxel_size,
                               self.coord_frame_resource.y_voxel_size, self.coord_frame_resource.z_voxel_size]
            self.voxel_unit = self.coord_frame_resource.voxel_unit
            self.datatype = self.ch_resource.datatype
            self.res = self.ch_resource.base_resolution
            self.img_size = [self.coord_frame_resource.x_stop,
                             self.coord_frame_resource.y_stop, self.coord_frame_resource.z_stop]

    def get_boss_project(self, proj_setup, get_only):
        try:
            proj_actual = self.rmt.get_project(proj_setup)
        except HTTPError:
            if get_only is False:
                try:
                    proj_actual = self.rmt.create_project(proj_setup)
                except Exception as e:
                    print(type(e), e)
                    raise(e)
            else:
                raise 'project not found with given params'
        except Exception as e:
            print(type(e), e)
            raise(e)
        return proj_actual

    def setup_boss_collection(self, get_only=True):
        coll_setup = CollectionResource(self.coll_name)
        return self.get_boss_project(coll_setup, get_only)

    def setup_boss_coord_frame(self, get_only=True):
        # if we don't know the coordinate frame parameters, get the one with the same name
        if get_only and (self.img_size is None or self.voxel_size is None or self.voxel_unit is None):
            coord_setup = CoordinateFrameResource(self.coord_frame_name)
        else:
            coord_setup = CoordinateFrameResource(self.coord_frame_name, '', 0, self.img_size[0], 0,
                                                  self.img_size[1], 0, self.img_size[2],
                                                  self.voxel_size[0], self.voxel_size[1], self.voxel_size[2], self.voxel_unit)
        return self.get_boss_project(coord_setup, get_only)

    def setup_boss_experiment(self, get_only=True):
        # if we don't know the coordinate frame parameters, get the one with the same name
        if get_only and (self.img_size is None or self.voxel_size is None):
            exp_setup = ExperimentResource(
                self.exp_name, self.coll_name, self.coord_frame_name)
        else:
            # if all elements are the same, isotropic, otherwise anisotropic
            if (len(set(self.voxel_size)) <= 1):
                hierarchy_method = 'isotropic'
            else:
                hierarchy_method = 'anisotropic'

            num_hierarchy_levels = calc_hierarchy_levels(self.img_size)

            exp_setup = ExperimentResource(self.exp_name, self.coll_name, self.coord_frame_name, '',
                                           num_hierarchy_levels, hierarchy_method)
        return self.get_boss_project(exp_setup, get_only)

    def setup_boss_channel(self, ch_type='image', ch_description='', get_only=True):
        if get_only and self.datatype is None:
            ch_setup = ChannelResource(
                self.ch_name, self.coll_name, self.exp_name)
        else:
            ch_setup = ChannelResource(
                self.ch_name, self.coll_name, self.exp_name, ch_type, ch_description, 0, self.datatype, self.res)
        return self.get_boss_project(ch_setup, get_only)


def calc_hierarchy_levels(img_size, lowest_res=64):
    max_xy = max(img_size[0:1])
    return math.ceil(math.log(max_xy / lowest_res, 2))
