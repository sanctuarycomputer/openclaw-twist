// Fast env-only "is this configured?" check used by the host before loading the
// full plugin. Must read only env / non-runtime inputs.
export function hasTwistConfiguredState(params) {
  const env = params?.env ?? {};
  return (
    typeof env.TWIST_TOKEN === "string" &&
    env.TWIST_TOKEN.trim().length > 0 &&
    typeof env.TWIST_WORKSPACE_ID === "string" &&
    env.TWIST_WORKSPACE_ID.trim().length > 0
  );
}
