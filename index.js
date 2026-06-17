// Twist channel plugin — runtime entry. Gateway-managed: the poll loop runs
// inside the gateway via the plugin's gateway.startAccount lifecycle.
import { defineChannelPluginEntry } from "openclaw/plugin-sdk/channel-core";
import { twistPlugin } from "./src/channel.js";
import { setTwistRuntime } from "./src/runtime.js";

export default defineChannelPluginEntry({
  id: "twist",
  name: "Twist",
  description: "Twist channel for OpenCLAW (poll-based DM/mention routing).",
  plugin: twistPlugin,
  setRuntime: setTwistRuntime,
});
