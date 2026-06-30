# Deploying the live visualizer

The visualizer (`cmd/raftviz`) is a single self-contained Go server - a 5-node Raft
cluster in one process plus a web UI. The repo-root [`Dockerfile`](../Dockerfile)
builds it for any container host. It listens on `$PORT` (default `7860`).

## Run it locally

```bash
go run ./cmd/raftviz      # then open http://localhost:7860
```

## Option A - Render (simplest; direct GitHub connect)

1. Push this repo to GitHub (already done).
2. On [render.com](https://render.com): **New → Web Service**, then give it the public repo URL `https://github.com/Jenak26/raftkv` (no GitHub connect needed for a public repo).
3. Set **Runtime / Environment: Docker** (Render finds the root `Dockerfile`). No build/start command needed.
4. Instance type: **Free**. Create.
5. Render assigns `$PORT` automatically and gives you a `https://<name>.onrender.com` URL.

> Free web services spin down after ~15 min idle and cold-start in ~30-60s on the next visit. Fine for a demo.

## Option B - Hugging Face Spaces (Docker SDK)

1. Create a new **Space** → SDK: **Docker** → blank.
2. Add this repo's `Dockerfile` and code to the Space repo, and put this front-matter at the top of the Space's `README.md`:

   ```yaml
   ---
   title: raftkv Live Raft Visualizer
   emoji: 🗳️
   colorFrom: blue
   colorTo: green
   sdk: docker
   app_port: 7860
   pinned: false
   ---
   ```
3. Push to the Space's git remote. HF builds the container and serves it at
   `https://<user>-<space>.hf.space`.

## Option C - Fly.io

```bash
fly launch --dockerfile Dockerfile --internal-port 7860   # accept defaults, then:
fly deploy
```

## After deploying

Add the URL to the README's **Live demo** section (replace the placeholder) so it's
one click from the top of the repo.
