fs = require "fs"
path = require "path"
{spawn, exec} = require "child_process"
stdout = process.stdout

# Use executables installed with npm bundle.
#process.env["PATH"] = "node_modules/.bin:#{process.env["PATH"]}"

# ANSI Terminal Colors.
bold = "\033[0;1m"
red = "\033[0;31m"
green = "\033[0;32m"
reset = "\033[0m"

# Log a message with a color.
log = (message, color, explanation) ->
  console.log color + message + reset + ' ' + (explanation or '')

# Handle error and kill the process.
onError = (err) ->
  if err
    process.stdout.write "#{red}#{err.stack}#{reset}\n"
    process.exit -1

# Setup development dependencies, not part of runtime dependencies.
task "setup", "Install development dependencies", ->
  fs.readFile "package.json", "utf8", (err, package) ->
    log "Need runtime dependencies, installing into node_modules ...", green
    exec "npm bundle", onError

    log "Need development dependencies, installing ...", green
    for name, version of JSON.parse(package).devDependencies
      log "Installing #{name} #{version}", green
      exec "npm bundle install \"#{name}@#{version}\"", onError

task "install", "Install Hoard in your local repository", ->
  build (err) ->
    onError err
    log "Installing Hoard ...", green
    exec "npm install", (err, stdout, stderr) ->
      process.stdout.write stderr
      onError err

build = (callback) ->
  log "Compiling CoffeeScript to JavaScript ...", green
  exec "coffee -c -l -b -o lib src", (err, stdout) -> callback err
task "build", "Compile CoffeeScript to JavaScript", -> build onError

runTests = (callback) ->
  log "Running test suite ...", green
  exec "expresso -I src -I lib test/*.test.coffee", (err, stdout, stderr) ->
    process.stdout.write stdout
    process.binding('stdio').writeError stderr
    callback err if callback

clean = (callback) ->
  log "Removing build files ...", green
  exec "rm -f test/*.hoard", (err, stdout) -> callback err
task "clean", "Clean up build files", -> clean onError

task "test", "Run all tests", ->
  runTests (err) ->
    process.stdout.on "drain", -> process.exit -1 if err
