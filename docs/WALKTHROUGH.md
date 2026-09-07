# App demonstration

`demo.gif` is converted from `demo.mp4`, a recording of the actual Dagster UI at
1280 × 720. The app runs locally with three days of generated web traffic.
Production services and records are not used. Pauses and navigation detours are
cut; the interface and execution results are real.

The recording follows these actions:

1. Inspect the source groups and their downstream dependencies.
2. Open `analytics_daily` and view its execution steps.
3. Launch the job, observe its run log, and see it succeed.
4. Open the catalog's folder view, then `lake` and `marts`.
5. Inspect `agg_traffic_daily`, its published file metadata and upstream SQL.
6. Open the table's checks and inspect the passing results.

The generated traffic demonstrates collection through publication. Camera,
LiteLLM and library models are visible, but their live collectors are not
connected in this demo. Their behavior is covered by synthetic test fixtures.

## Record it again

Run `bash scripts/demo.sh`, source `scripts/demo-env.sh`, then start `dagster dev`.
Record only the browser window while performing the actions above. Inspect all
footage for personal data before saving the MP4. Convert the recording with:

```sh
ffmpeg -i demo.mp4 -vf 'fps=6,split[a][b];[a]palettegen[p];[b][p]paletteuse=dither=bayer:bayer_scale=3' -loop 0 demo.gif
```

No generated illustrations or simulated application screens are used.
