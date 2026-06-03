UPDATE exposures
SET status = 3,
    updated_at = NOW()
WHERE status = 2
  AND (
      openziti_service_id = ''
      OR openziti_bind_policy_id = ''
      OR openziti_dial_policy_id = ''
      OR url = ''
  );

ALTER TABLE exposures
ADD CONSTRAINT exposures_active_resources_check
CHECK (
    status <> 2
    OR (
        openziti_service_id <> ''
        AND openziti_bind_policy_id <> ''
        AND openziti_dial_policy_id <> ''
        AND url <> ''
    )
);
