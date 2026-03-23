ALTER TABLE projects
ADD COLUMN IF NOT EXISTS project_id TEXT,
ADD COLUMN IF NOT EXISTS project_name TEXT;

UPDATE projects
SET project_id = COALESCE(project_id, 'prj_' || lpad(id::text, 8, '0')),
    project_name = COALESCE(project_name, project_id);

ALTER TABLE projects
ALTER COLUMN project_id SET NOT NULL;

ALTER TABLE projects
ALTER COLUMN project_name SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_projects_project_id
ON projects(project_id);
