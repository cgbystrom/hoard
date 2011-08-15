assert = require 'assert'
fs = require 'fs'
path = require 'path'
hoard = require "hoard"
equal = assert.equal

FILENAME = 'test/large.whisper'
unixTime = -> parseInt(new Date().getTime() / 1000)

# Tests against Python generated Whisper data file
module.exports =
  'test info()': (beforeExit) ->
    called = false
    hoard.info FILENAME, (err, header) ->
      called = true
      assert.equal 94608000, header.maxRetention
      assert.equal 0.5, header.xFilesFactor
      assert.equal 2, header.archives.length

      archive = header.archives[0]
      assert.equal 31536000, archive.retention
      assert.equal 3600, archive.secondsPerPoint
      assert.equal 8760, archive.points
      assert.equal 105120, archive.size
      assert.equal 40, archive.offset

      archive = header.archives[1]
      assert.equal 94608000, archive.retention
      assert.equal 86400, archive.secondsPerPoint
      assert.equal 1095, archive.points
      assert.equal 13140, archive.size
      assert.equal 105160, archive.offset

    beforeExit -> assert.ok called

  'test fetch()': (beforeExit) ->
    called = false
    fromTime = 1311161605
    toTime = 1311179605

    hoard.fetch FILENAME, fromTime, toTime, (err, timeInfo, values) ->
      throw err if err
      called = true
      assert.equal 1311163200, timeInfo[0]
      assert.equal 1311181200, timeInfo[1]
      assert.equal 3600, timeInfo[2]
      v = [2048, 4546, 794, 805, 4718]
      assert.length values, v.length
      assert.eql v, values

    beforeExit -> assert.ok called, 'Callback must return'

  'test create()': (beforeExit) ->
    called = false
    filename = 'test/testcreate.hoard'
    if path.existsSync(filename) then fs.unlinkSync(filename)

    hoard.create filename, [[1, 60], [10, 600]], 0.5, (err) ->
      if err then throw err

      hoardFile = fs.readFileSync(filename)
      whisperFile = fs.readFileSync('test/testcreate.whisper')
      equal whisperFile.length, hoardFile.length, "File lengths must match"

      hoard.info filename, (err, header) ->
        called = true
        assert.equal 6000, header.maxRetention
        assert.equal 0.5, header.xFilesFactor
        assert.equal 2, header.archives.length

        archive = header.archives[0]
        assert.equal 60, archive.retention
        assert.equal 1, archive.secondsPerPoint
        assert.equal 60, archive.points
        assert.equal 720, archive.size
        assert.equal 40, archive.offset

        archive = header.archives[1]
        assert.equal 6000, archive.retention
        assert.equal 10, archive.secondsPerPoint
        assert.equal 600, archive.points
        assert.equal 7200, archive.size
        assert.equal 760, archive.offset

      # FIXME: Compare to real file, must mock creation timestamp in create()
      #assert.eql whisperFile, hoardFile

    beforeExit -> assert.ok called, "Callback must return"

  'test update()': (beforeExit) ->
    called = false
    filename = 'test/testupdate.hoard'
    if path.existsSync(filename) then fs.unlinkSync(filename)

    hoard.create filename, [[3600, 8760], [86400, 1095]], 0.5, (err) ->
      if err then throw err
      hoard.update filename, 1337, 1311169605, (err) ->
        if err then throw err
        hoard.fetch filename, 1311161605, 1311179605, (err, timeInfo, values) ->
          if err then throw err
          called = true
          equal 1311163200, timeInfo[0]
          equal 1311181200, timeInfo[1]
          equal 3600, timeInfo[2]
          assert.length values, 5
          equal 1337, values[1]

    beforeExit -> assert.ok called, "Callback must return"

  'test updateMany()': (beforeExit) ->
    called = false
    filename = 'test/testupdatemany.hoard'
    if path.existsSync(filename) then fs.unlinkSync(filename)

    tsData = JSON.parse(fs.readFileSync('test/timeseriesdata.json', 'utf8'))
    console.log tsData[0]
    hoard.create filename, [[3600, 8760], [86400, 1095]], 0.5, (err) ->
      if err then throw err
      hoard.updateMany filename, tsData, (err) ->
        if err then throw err
        from = 1311277105
        to =   1311295105
        hoard.fetch filename, from, to, (err, timeInfo, values) ->
          if err then throw err
          called = true
          equal 1311278400, timeInfo[0]
          equal 1311296400, timeInfo[1]
          equal 3600, timeInfo[2]
          assert.length values, 5
          assert.eql [1043, 3946, 1692, 899, 2912], values

    beforeExit -> assert.ok called, "Callback must return"
