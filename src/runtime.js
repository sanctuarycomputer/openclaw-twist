// Stores the PluginRuntime injected by the gateway so inbound/outbound code can
// reach core.channel.* helpers (inbound dispatch, session recording, mentions).
// Mirrors the bundled channels' runtime-store pattern.
import { createPluginRuntimeStore } from "openclaw/plugin-sdk/runtime-store";

const store = createPluginRuntimeStore({
  channel: "twist",
  errorMessage: "Twist runtime not initialized",
});

export const setTwistRuntime = store.setRuntime;
export const getTwistRuntime = store.getRuntime;
export const clearTwistRuntime = store.clearRuntime;
