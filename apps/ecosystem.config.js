module.exports = {
  apps: [
    {
      name: "collector",
      cwd: __dirname + "/collector",
      script: "node",
      args: ["dist/index.js"],
      autorestart: true,
      restart_delay: 2000,
      env: { NODE_ENV: "production" }
    },
    {
      name: "builder",
      cwd: __dirname + "/builder",
      script: "node",
      args: ["dist/index.js"],
      autorestart: true,
      restart_delay: 2000,
      env: { NODE_ENV: "production" }
    },
    {
      name: "sinker",
      cwd: __dirname + "/sinker",
      script: "node",
      args: ["dist/index.js"],
      autorestart: true,
      restart_delay: 2000,
      env: { NODE_ENV: "production" }
    },
    {
      name: "viewer",
      cwd: __dirname + "/viewer",
      script: "node_modules/next/dist/bin/next",
      args: ["start", "-p", "3000"],
      autorestart: true,
      restart_delay: 2000,
      env: { NODE_ENV: "production", PORT: "3000" }
    },
    {
      name: "viewer-ws",
      cwd: __dirname + "/viewer",
      script: "node",
      args: ["dist/scripts/viewer-ws.js"],
      autorestart: true,
      restart_delay: 2000,
      env: {
        NODE_ENV: "production",
        WS_HOST: "0.0.0.0",
        PORT_WS: "3101"
      }
    }
  ]
};
