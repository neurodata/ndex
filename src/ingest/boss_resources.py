'''
Class for interacting with the BOSS
Stores BOSS resources
Associated with an ingest job
'''

import math

from requests import HTTPError

from intern.remote.boss import BossRemote
from intern.resource.boss.resource import *
from intern.service.boss.httperrorlist import HTTPErrorList


class BossResParams:
    def __init__(self, ingest_job, get_only=True):
        self.ingest_job = ingest_job

        self.coord_frame_name = '_'.join(
            (ingest_job.coll_name, ingest_job.exp_name))

        self.rmt = BossRemote(self.ingest_job.boss_config_file)

        self.coll_resource = self.setup_boss_collection(get_only=get_only)

        self.coord_frame_resource = self.setup_boss_coord_frame(
            get_only=get_only)

        self.exp_resource = self.setup_boss_experiment(get_only=get_only)

        self.ch_resource = self.setup_boss_channel(get_only=get_only)

    def get_boss_project(self, proj_setup, get_only):
        try:
            proj_actual = self.rmt.get_project(proj_setup)
        except HTTPError as e:
            if get_only is False:
                try:
                    proj_actual = self.rmt.create_project(proj_setup)
                except Exception as e:
                    print(type(e), e)
                    raise(e)
            else:
                print(type(e), e)
                raise e
        except Exception as e:
            print(type(e), e)
            raise(e)
        return proj_actual

    def setup_boss_collection(self, get_only=True):
        coll_setup = CollectionResource(self.ingest_job.coll_name)
        return self.get_boss_project(coll_setup, get_only)

    def setup_boss_coord_frame(self, get_only=True):
        # if we don't know the coordinate frame parameters, get the one with the same name
        if get_only:
            coord_setup = CoordinateFrameResource(self.coord_frame_name)
        else:
            coord_setup = CoordinateFrameResource(self.coord_frame_name, '',
                                                  self.ingest_job.coord_frame_x_extent[0],
                                                  self.ingest_job.coord_frame_x_extent[1],
                                                  self.ingest_job.coord_frame_y_extent[0],
                                                  self.ingest_job.coord_frame_y_extent[1],
                                                  self.ingest_job.coord_frame_z_extent[0],
                                                  self.ingest_job.coord_frame_z_extent[1],
                                                  self.ingest_job.voxel_size[0],
                                                  self.ingest_job.voxel_size[1],
                                                  self.ingest_job.voxel_size[2],
                                                  self.ingest_job.voxel_unit)
        coord_frame_resource = self.get_boss_project(coord_setup, get_only)

        if get_only:
            # matching ingest_job values to coordinate frame values (if they weren't specified, they are now populated)
            self.ingest_job.voxel_size = [coord_frame_resource.x_voxel_size,
                                          coord_frame_resource.y_voxel_size,
                                          coord_frame_resource.z_voxel_size]
            self.ingest_job.voxel_unit = coord_frame_resource.voxel_unit

        return coord_frame_resource

    def setup_boss_experiment(self, get_only=True):
        # if we don't know the coordinate frame parameters, get the one with the same name
        if get_only:
            exp_setup = ExperimentResource(
                self.ingest_job.exp_name, self.ingest_job.coll_name, self.coord_frame_name)
        else:
            # if all elements are the same, isotropic, otherwise anisotropic
            if len(set(self.ingest_job.voxel_size)) <= 1:
                hierarchy_method = 'isotropic'
            else:
                hierarchy_method = 'anisotropic'

            num_hierarchy_levels = self.calc_hierarchy_levels()

            exp_setup = ExperimentResource(self.ingest_job.exp_name, self.ingest_job.coll_name, self.coord_frame_name, '',
                                           num_hierarchy_levels, hierarchy_method)
        exp_resource = self.get_boss_project(exp_setup, get_only)

        # record the offset (if there is any) into BOSS metadata field for experiment
        self.record_offsets(exp_resource)

        return exp_resource

    def record_offsets(self, exp_resource):
        if any([a > 0 for a in self.ingest_job.offsets]):
            offsets_dict = {'offsets': self.ingest_job.offsets}
            try:
                self.rmt.create_metadata(exp_resource, offsets_dict)
            except HTTPErrorList:  # keys already exist
                self.rmt.update_metadata(exp_resource, offsets_dict)

    def setup_boss_channel(self, ch_description='', get_only=True):
        ch_args = [self.ingest_job.ch_name,
                   self.ingest_job.coll_name, self.ingest_job.exp_name]
        if not get_only:
            ch_args.extend((self.ingest_job.ch_type, ch_description,
                            0, self.ingest_job.boss_datatype, self.ingest_job.res))
            if self.ingest_job.source_channel is not None:
                # annotation data
                ch_args.append([self.ingest_job.source_channel])
        ch_setup = ChannelResource(*ch_args)
        ch_resource = self.get_boss_project(ch_setup, get_only)

        if get_only:
            self.ingest_job.boss_datatype = ch_resource.datatype
            if self.ingest_job.datatype is None:
                self.ingest_job.datatype = ch_resource.datatype
            self.ingest_job.res = ch_resource.base_resolution

        return ch_resource

    def calc_hierarchy_levels(self, lowest_res=512):
        img_size = [self.coord_frame_resource.x_stop - self.coord_frame_resource.x_start,
                    self.coord_frame_resource.y_stop - self.coord_frame_resource.y_start,
                    self.coord_frame_resource.z_stop - self.coord_frame_resource.z_start]

        min_xy = min(img_size[0:1])
        # we add one because 0 is included in the number of downsampling levels
        num_levels = math.ceil(math.log(min_xy / lowest_res, 2)) + 1
        return num_levels
