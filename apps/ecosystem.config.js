// ecosystem.config.js
module.exports = {
  apps: [
    // 1) collector
    {
      name: "collector",
      cwd: __dirname,
      script: "node",
      args: ["collector/run/collector.js"], // ✅ 네 실제 엔트리로 수정
      autorestart: true,
      max_restarts: 50,
      restart_delay: 2000,
      time: true,
      env: {
        NODE_ENV: "production",
      },
    },

    // 2) builder
    {
      name: "builder",
      cwd: __dirname,
      script: "node",
      args: ["builder/run/builder.js"], // ✅ 수정
      autorestart: true,
      max_restarts: 50,
      restart_delay: 2000,
      time: true,
      env: {
        NODE_ENV: "production",
      },
    },

    // 3) sinker
    {
      name: "sinker",
      cwd: __dirname,
      script: "node",
      args: ["sinker/run/sinker.js"], // ✅ 수정
      autorestart: true,
      max_restarts: 50,
      restart_delay: 2000,
      time: true,
      env: {
        NODE_ENV: "production",
      },
    },

    // 4) viewer (Next.js 서버)
    // 빌드는 미리 해두고 "next start"로 띄우는 게 정석
    {
      name: "viewer",
      cwd: __dirname,
      script: "node_modules/next/dist/bin/next",
      args: ["start", "-p", "3000"], // ✅ 포트 수정 가능
      autorestart: true,
      max_restarts: 50,
      restart_delay: 2000,
      time: true,
      env: {
        NODE_ENV: "production",
        PORT: "3000",
      },
    },

    // 5) viewer-ws (웹소켓 서버)
    {
      name: "viewer-ws",
      cwd: __dirname,
      script: "node",
      args: ["viewer/viewer-ws.js"], // ✅ 수정
      autorestart: true,
      max_restarts: 50,
      restart_delay: 2000,
      time: true,
      env: {
        NODE_ENV: "production",
        PORT_WS: "3101", // ✅ 네 구성에 맞게
      },
    },
  ],
};
