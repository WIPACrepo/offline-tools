insert into datasets (dataset_id, description, season, type, enabled, working_group, comment, iceprod_id) values (@L2, "IC86 2022 Offline L2 Data Production", 2022, "L2", 1, 0, "IceProd2", @L2i ) ;
# update datasets set enabled = 0 where dataset_id = @L2x ;
select * from datasets where dataset_id = @L2x or dataset_id = @L2 ;


insert into datasets (dataset_id, description, season, type, enabled, working_group, comment, iceprod_id) values (@C3, "IC86 2022 L3 Data Production for the Cascade WG", 2022, "L3", 1, 3, "IceProd2", @C3i ) ;
# update datasets set enabled = 0 where dataset_id = @C3x ;
select * from datasets where dataset_id = @C3x or dataset_id = @C3 ;

insert into source_dataset_id (dataset_id, source_dataset_id) values (@C3, @L2) ;
select * from source_dataset_id where dataset_id = @C3 ;

insert into level3_config (dataset_id, path, aggregate) values (@C3, '/data/ana/Cscd/IC86-{season}/level3/exp/{year}/{month:0>2}{day:0>2}/Run{run_id:0>8}', 1) ;
select * from level3_config where dataset_id = @C3x or dataset_id = @C3 ;


insert into datasets (dataset_id, description, season, type, enabled, working_group, comment, iceprod_id) values (@M3, "IC86 2022 L3 Data Production for the Muon WG", 2022, "L3", 1, 1, "IceProd2", @M3i ) ;
# update datasets set enabled = 0 where dataset_id = @M3x ;
select * from datasets where dataset_id = @M3x or dataset_id = @M3 ;

insert into source_dataset_id (dataset_id, source_dataset_id) values (@M3, @L2) ;
select * from source_dataset_id where dataset_id = @M3 ;

insert into level3_config (dataset_id, path, aggregate) values (@M3, '/data/ana/Muon/level3/exp/{year}/{month:02d}{day:02d}/Run{run_id:08d}', 1) ;
select * from level3_config where dataset_id = @M3x or dataset_id = @M3 ;


insert into datasets (dataset_id, description, season, type, enabled, working_group, comment, iceprod_id) values (@L4, "IC86 2022 L4 Data Production for the Diffuse/Atmospheric Neutrinos WG", 2022, "L4", 1, 5, "IceProd2", @L4i ) ;
# update datasets set enabled = 0 where dataset_id = @L4x ;
select * from datasets where dataset_id = @L4x or dataset_id = @L4 ;

insert into source_dataset_id (dataset_id, source_dataset_id) values (@L4, @M3) ;
select * from source_dataset_id where dataset_id = @L4 ;

insert into level3_config (dataset_id, path, aggregate) values (@L4, '/data/ana/Diffuse/AachenUpgoingTracks/exp/Pass2_i3filter/{year}/{month:02d}{day:02d}/Run{run_id:08d}', 20) ;
select * from level3_config where dataset_id = @L4x or dataset_id = @L4 ;
