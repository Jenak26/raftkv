# raftkv live demo - render to a GIF with: vhs docs/demo.tape
# Requires https://github.com/charmbracelet/vhs (and ffmpeg). Writes docs/demo.gif.
# Run from the repo root. Binaries go to ./bin and data to ./data (both gitignored).

Output docs/demo.gif

Set Shell "bash"
Set FontSize 16
Set Width 1200
Set Height 760
Set Padding 18
Set Theme "Dracula"

Type "# Build the node and CLI binaries"
Enter
Type "go build -o ./bin/kvserver ./cmd/kvserver && go build -o ./bin/kvctl ./cmd/kvctl"
Enter
Sleep 6s

Type "# Start a 3-node cluster in the background"
Enter
Type 'PEERS=0=127.0.0.1:9000,1=127.0.0.1:9001,2=127.0.0.1:9002'
Enter
Type './bin/kvserver -id 0 -peers $PEERS -data ./data/0 >/dev/null 2>&1 &'
Enter
Type './bin/kvserver -id 1 -peers $PEERS -data ./data/1 >/dev/null 2>&1 &'
Enter
Type './bin/kvserver -id 2 -peers $PEERS -data ./data/2 >/dev/null 2>&1 &'
Enter
Sleep 2s

Type 'SRV=127.0.0.1:9000,127.0.0.1:9001,127.0.0.1:9002'
Enter
Type "# Write and read a key"
Enter
Type './bin/kvctl -servers $SRV put color blue'
Enter
Sleep 1s
Type './bin/kvctl -servers $SRV get color'
Enter
Sleep 1500ms

Type "# Kill a node - the cluster keeps serving on the surviving majority"
Enter
Type "kill %1"
Enter
Sleep 1500ms
Type './bin/kvctl -servers $SRV cas color blue green'
Enter
Sleep 1s
Type './bin/kvctl -servers $SRV get color'
Enter
Sleep 2s

Type "# Clean up"
Enter
Type "kill %2 %3 2>/dev/null; rm -rf ./data ./bin"
Enter
Sleep 1500ms
