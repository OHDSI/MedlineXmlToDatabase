-- Some recommended indices
CREATE INDEX idx_medcit_meshheadinglist_meshheading_descriptorname_ui ON medcit_meshheadinglist_meshheading (descriptorname_ui);
CREATE INDEX idx_medcit_supplmeshlist_supplmeshname_ui ON medcit_supplmeshlist_supplmeshname (ui);
CREATE INDEX idx_mesh_term_ui ON mesh_term (ui);
CREATE INDEX idx_mesh_relationship_ui_1 ON mesh_relationship (ui_1);
CREATE INDEX idx_mesh_relationship_ui_2 ON mesh_relationship (ui_2);
CREATE INDEX idx_mesh_ancestor_ancestor_ui ON mesh_ancestor (ancestor_ui);
CREATE INDEX idx_mesh_ancestor_descendant_ui ON mesh_ancestor (descendant_ui);