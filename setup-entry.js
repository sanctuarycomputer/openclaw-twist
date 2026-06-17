// Lightweight setup entry — used when the channel is disabled/unconfigured so the
// host can show setup status without loading the full runtime/CLI surface.
import { defineSetupPluginEntry } from "openclaw/plugin-sdk/channel-core";
import { twistPlugin } from "./src/channel.js";

export default defineSetupPluginEntry(twistPlugin);
