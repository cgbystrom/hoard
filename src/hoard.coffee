fs = require('fs')
Buffer = require('buffer').Buffer
Binary = require('binary')
_ = underscore = require('underscore')
async = require('async')
pack = require('jspack').jspack
path = require('path')
Put = require('put')


# Monkey patch since modulo operator is broken in JS
Number.prototype.mod = (n) -> ((@ % n) + n) % n

longFormat = "!L"
longSize = pack.CalcLength(longFormat)
floatFormat = "!f"
floatSize = pack.CalcLength(floatFormat)
timestampFormat = "!L"
timestampSize = pack.CalcLength(timestampFormat)
valueFormat = "!d"
valueSize = pack.CalcLength(valueFormat)
pointFormat = "!Ld"
pointSize = pack.CalcLength(pointFormat)
metadataFormat = "!2LfL"
metadataSize = pack.CalcLength(metadataFormat)
archiveInfoFormat = "!3L"
archiveInfoSize = pack.CalcLength(archiveInfoFormat)

# FIXME: Determine this in a better way
POINT_SIZE = 4 + 8 # long, double

unixTime = -> parseInt(new Date().getTime() / 1000)

create = (filename, archives, xFilesFactor, cb) ->
    # FIXME: Check parameters
    # FIXME: Check that values are correctly formatted
    archives.sort (a, b) -> a[0] - b[0]

    if path.existsSync(filename)
        cb new Error('File ' + filename + ' already exists')

    oldest = (a[0] * a[1] for a in archives).sort().reverse()[0]

    encodeFloat = (value) ->
        # Dirty hack.
        # Using 'buffer_ieee754' from node 0.5.x
        # as no libraries had a working IEEE754 encoder
        buffer = new Buffer(4)
        require('buffer_ieee754').writeIEEE754(buffer, 0.5, 0, 'big', 23, 4);
        buffer

    buffer = Put()
        .word32be(unixTime()) # last update
        .word32be(oldest) # max retention
        .put(encodeFloat(xFilesFactor))
        .word32be(archives.length)

    headerSize = metadataSize + (archiveInfoSize * archives.length)
    archiveOffset = headerSize

    for archive in archives
        secondsPerPoint = archive[0]; points = archive[1]
        buffer.word32be(archiveOffset)
        buffer.word32be(secondsPerPoint)
        buffer.word32be(points)
        archiveOffset += (points * pointSize)

    # Pad archive data itself with zeroes
    buffer.pad(archiveOffset - headerSize)

    # FIXME: Check file lock?
    # FIXME: fsync this?
    fs.writeFile filename, buffer.buffer(), 'binary', cb

propagate = (fd, timestamp, xff, higher, lower, cb) ->
    lowerIntervalStart = timestamp - timestamp.mod(lower.secondsPerPoint)
    lowerIntervalEnd = lowerIntervalStart + lower.secondsPerPoint

    packedPoint = new Buffer(pointSize)
    fs.read fd, packedPoint, 0, pointSize, higher.offset, (err, written, buffer) ->
        cb(err) if err
        [higherBaseInterval, higherBaseValue] = pack.Unpack(pointFormat, packedPoint)

        if higherBaseInterval == 0
            higherFirstOffset = higher.offset
        else
            timeDistance = lowerIntervalStart - higherBaseInterval
            pointDistance = timeDistance / higher.secondsPerPoint
            byteDistance = pointDistance * pointSize
            higherFirstOffset = higher.offset + byteDistance.mod(higher.size)

        higherPoints = lower.secondsPerPoint / higher.secondsPerPoint
        higherSize = higherPoints * pointSize
        relativeFirstOffset = higherFirstOffset - higher.offset
        relativeLastOffset = (relativeFirstOffset + higherSize).mod(higher.size)
        higherLastOffset = relativeLastOffset + higher.offset

        # !!!!!!!!!!!!!!!!!
        #fh.seek(higherFirstOffset)
        #console.log 'seriesSize'
        #console.log 'seriesSize', 123
        if higherFirstOffset < higherLastOffset
            # We don't wrap the archive

            seriesSize = higherLastOffset - higherFirstOffset

            seriesString = new Buffer(seriesSize)

            #console.log 'seriesSize', seriesSize
            #console.log 'seriesSize', seriesSize

            fs.read fd, seriesString, 0, seriesSize, higherFirstOffset, (err, written, buffer) ->
                parseSeries(seriesString)
            #parseSeries(seriesString)

        else
            console.log 'seriesSize', 124
            # We do wrap the archive
            higherEnd = higher.offset + higher.size
            # !!!!!!!!!!!!!!!!!
            firstSeriesSize = higherEnd - higherFirstOffset
            secondSeriesSize = higherLastOffset - higher.offset

            seriesString = new Buffer(firstSeriesSize + secondSeriesSize)

            console.log 'seriesSize here', (firstSeriesSize + secondSeriesSize)
            console.log 'higherFirstOffset', higherFirstOffset
            console.log 'firstSeriesSize', firstSeriesSize
            console.log 'ASDASD'
            fs.read fd, seriesString, 0, firstSeriesSize, higherFirstOffset, (err, written, buffer) ->
                cb(err) if err
                console.log 'higher.offset', higher.offset
                console.log 'firstSeriesSize', firstSeriesSize
                console.log 'secondSeriesSize', secondSeriesSize
                if secondSeriesSize > 0
                    fs.read fd, seriesString, firstSeriesSize, secondSeriesSize, higher.offset, (err, written, buffer) ->
                        cb(err) if err
                        parseSeries(seriesString)
                else
                    ret = new Buffer(firstSeriesSize)
                    seriesString.copy(ret, 0, 0, firstSeriesSize)
                    parseSeries(ret)

    parseSeries = (seriesString) ->
        # Now we unpack the series data we just read
        [byteOrder, pointTypes] = [pointFormat[0], pointFormat.slice(1)]
        points = seriesString.length / pointSize

        seriesFormat = byteOrder + (pointTypes for f in [0...points]).join("")
        unpackedSeries = pack.Unpack(seriesFormat, seriesString, 0)

        # And finally we construct a list of values
        neighborValues = (null for f in [0...points])
        currentInterval = lowerIntervalStart
        step = higher.secondsPerPoint

        for i in [0...unpackedSeries.length] by 2
            pointTime = unpackedSeries[i]
            if pointTime == currentInterval
                neighborValues[i/2] = unpackedSeries[i+1]
            currentInterval += step



        # Propagate aggregateValue to propagate from neighborValues if we have enough known points
        knownValues = (v for v in neighborValues when v isnt null)
        if knownValues.length == 0
            cb null, false
            return

        sum = (list) ->
            s = 0
            for x in list
                s += x
            s

        knownPercent = knownValues.length / neighborValues.length
        if knownPercent >= xff
            # We have enough data to propagate a value!
            aggregateValue = sum(knownValues) / knownValues.length # TODO: Another CF besides average?
            myPackedPoint = pack.Pack(pointFormat, [lowerIntervalStart, aggregateValue])

            # !!!!!!!!!!!!!!!!!
            packedPoint = new Buffer(pointSize)
            fs.read fd, packedPoint, 0, pointSize, lower.offset, (err) ->
                [lowerBaseInterval, lowerBaseValue] = pack.Unpack(pointFormat, packedPoint)

                if lowerBaseInterval == 0
                    # First propagated update to this lower archive
                    offset = lower.offset
                else
                    # Not our first propagated update to this lower archive
                    timeDistance = lowerIntervalStart - lowerBaseInterval
                    pointDistance = timeDistance / lower.secondsPerPoint
                    byteDistance = pointDistance * pointSize
                    offset = lower.offset + byteDistance.mod(lower.size)

                #console.log 'myPackedPoint', myPackedPoint
                #console.log 'pointSize', pointSize
                #console.log 'offset', offset
                mypp = new Buffer(myPackedPoint)
                fs.write fd, mypp, 0, pointSize, offset, (err) ->
                    #console.log 'apa'
                    cb(null, true)
        else
            cb(null, false)


update = (filename, value, timestamp, cb) ->
    # FIXME: Check file lock?
    # FIXME: Don't use info(), re-use fd between internal functions
    info filename, (err, header) ->
        cb(err) if err
        now = unixTime()
        diff = now - timestamp
        if not (diff < header.maxRetention and diff >= 0)
            cb(new Error('Timestamp not covered by any archives in this database.'))
            return

        #console.log('Header', header)

        # Find the highest-precision archive that covers timestamp
        for i in [0...header.archives.length]
            archive = header.archives[i]
            continue if archive.retention < diff
            # We'll pass on the update to these lower precision archives later
            lowerArchives = header.archives.slice(i + 1)
            break

        fs.open filename, 'r+', (err, fd) ->
            cb(err) if err
            # First we update the highest-precision archive
            myInterval = timestamp - timestamp.mod(archive.secondsPerPoint)
            myPackedPoint = new Buffer(pack.Pack(pointFormat, [myInterval, value]))

            packedPoint = new Buffer(pointSize)
            fs.read fd, packedPoint, 0, pointSize, archive.offset, (err, bytesRead, buffer) ->
                cb(err) if err
                [baseInterval, baseValue] = pack.Unpack(pointFormat, packedPoint)

                if baseInterval == 0
                    # This file's first update
                    fs.write fd, myPackedPoint, 0, pointSize, archive.offset, (err, written, buffer) ->
                        cb(err) if err
                        [baseInterval, baseValue] = [myInterval, value]
                        propagateLowerArchives()
                else
                    # File has been updated before
                    timeDistance = myInterval - baseInterval
                    pointDistance = timeDistance / archive.secondsPerPoint
                    byteDistance = pointDistance * pointSize
                    myOffset = archive.offset + byteDistance.mod(archive.size)
                    fs.write fd, myPackedPoint, 0, pointSize, myOffset, (err, written, buffer) ->
                        cb(err) if err
                        propagateLowerArchives()

            propagateLowerArchives = ->
                # Propagate the update to lower-precision archives
                #higher = archive
                #for lower in lowerArchives:
                #    if not __propagate(fd, myInterval, header.xFilesFactor, higher, lower):
                #        break
                #    higher = lower

                #__changeLastUpdate(fh)

                # FIXME: Also fsync here?
                fs.close fd, cb
    return

updateMany = (filename, points, cb) ->
    points.sort((a, b) -> a[0] - b[0]).reverse()
    # FIXME: Check lock
    info filename, (err, header) ->
        cb err if err
        fs.open filename, 'r+', (err, fd) ->
            now = unixTime()
            archives = header.archives
            currentArchiveIndex = 0
            currentArchive = header.archives[currentArchiveIndex]
            currentPoints = []

            console.log 'HEJ'
            updateArchiveCalls = []
            for point in points
                age = now - point[0]

                while currentArchive.retention < age # We can't fit any more points in this archive
                    if currentPoints
                        # Commit all the points we've found that it can fit
                        currentPoints.reverse() # Put points in chronological order
                        do (header, currentArchive, currentPoints) ->
                            f = (cb) -> updateManyArchive fd, header, currentArchive, currentPoints, cb
                            updateArchiveCalls.push(f)
                        currentPoints = []

                    if currentArchiveIndex < (archives.length - 1)
                        currentArchiveIndex++
                        currentArchive = archives[currentArchiveIndex]
                    else
                        # Last archive
                        currentArchive = null
                        break

                if not currentArchive
                    break # Drop remaining points that don't fit in the database

                currentPoints.push(point)

            console.log 'last uac'
            async.series updateArchiveCalls, (err, results) ->
                throw err if err
                console.log 'async series 1'
                if currentArchive and currentPoints.length > 0
                    # Don't forget to commit after we've checked all the archives
                    currentPoints.reverse()
                    console.log 'async series 2'
                    updateManyArchive fd, header, currentArchive, currentPoints, (err) ->
                        console.log 'async series 3'
                        throw err if err
                        fs.close fd, cb
                else
                    console.log 'DONE IN UAC'
                    fs.close fd, cb

            # FIXME: touch last update
            # FIXME: fsync here?
            # FIXME: close fd fh.close()
        #cb(null)



updateManyArchive = (fd, header, archive, points, cb) ->
    step = archive.secondsPerPoint
    alignedPoints = []
    #alignedPoints = [(timestamp - (timestamp % step), value) for (timestamp,value) in points]
    for p in points
        [timestamp, value] = p
        alignedPoints.push([timestamp - timestamp.mod(step), value])


    # Create a packed string for each contiguous sequence of points
    packedStrings = []
    previousInterval = null
    currentString = []

    #console.log 'ap', alignedPoints
    for ap in alignedPoints
        [interval, value] = ap

        if !previousInterval or (interval == previousInterval + step)
            currentString.concat(pack.Pack(pointFormat, [interval, value]))
            previousInterval = interval
        else
            numberOfPoints = currentString.length / pointSize
            startInterval = previousInterval - (step * (numberOfPoints - 1))
            packedStrings.push([startInterval, new Buffer(currentString)])
            currentString = pack.Pack(pointFormat, [interval, value])
            previousInterval = interval

    if currentString.length > 0
        numberOfPoints = currentString.length / pointSize
        startInterval = previousInterval - (step * (numberOfPoints - 1))
        console.log 'packing', startInterval, currentString
        packedStrings.push([startInterval, new Buffer(currentString, 'binary')])

    # FIXME!!!
    # currentString is an array with tuples of arrayer of data
    # convert to buffers before writing

    console.log 'APA1'
    # Read base point and determine where our writes will start
    packedBasePoint = new Buffer(pointSize)
    fs.read fd, packedBasePoint, 0, pointSize, archive.offset, (err) ->
        cb err if err
        console.log 'APA2'
        [baseInterval, baseValue] = pack.Unpack(pointFormat, packedBasePoint)

        if baseInterval == 0
            # This file's first update
            # Use our first string as the base, so we start at the start
            baseInterval = packedStrings[0][0]

        # Write all of our packed strings in locations determined by the baseInterval

        writePackedString = (ps, callback) ->
            [interval, packedString] = ps
            timeDistance = interval - baseInterval
            pointDistance = timeDistance / step
            byteDistance = pointDistance * pointSize
            myOffset = archive.offset + byteDistance.mod(archive.size)
            archiveEnd = archive.offset + archive.size
            bytesBeyond = (myOffset + packedString.length) - archiveEnd

            #console.log 'APA3'
            #fh.seek(myOffset)
            if bytesBeyond > 0
                # FIXME fh.write( packedString[:-bytesBeyond] ) # Everything but not last bytesBeyond
                #console.log 'APA4'
                fs.write fd, packedString, 0, packedString.length - bytesBeyond, myOffset, (err) ->
                    cb err if err
                    #console.log 'APA4.1'
                    assert.equal archiveEnd, myOffset + packedString.length - bytesBeyond
                    #assert fh.tell() == archiveEnd, "archiveEnd=%d fh.tell=%d bytesBeyond=%d len(packedString)=%d" % (archiveEnd,fh.tell(),bytesBeyond,len(packedString))
                    # Safe because it can't exceed the archive (retention checking logic above)
                    fs.write fd, packedString, packedString.length - bytesBeyond, bytesBeyond, archive.offset, (err) ->
                        cb err if err
                        #console.log 'APA4.2'
                        callback()
            else
                ## FIXME ALL THIS SHIT IT IS A looping, ARGH WTF
                #console.log 'APA5'
                #console.log 'ps', ps
                #console.log 'packedString', packedString
                fs.write fd, packedString, 0, packedString.length, myOffset, (err) ->
                    #console.log "HELLO?"
                    callback()

        async.forEachSeries packedStrings, writePackedString, (err) ->
            throw err if err
            console.log 'Propagating to lower...'
            propagateLowerArchives()


        propagateLowerArchives = ->
            # Now we propagate the updates to lower-precision archives
            higher = archive
            lowerArchives = (arc for arc in header.archives when arc.secondsPerPoint > archive.secondsPerPoint)
            console.log 'prop1'

            if lowerArchives.length > 0
                # Collect a list of propagation calls to make
                # This is easier than doing async looping
                propagateCalls = []
                console.log 'prop2'
                for lower in lowerArchives
                    fit = (i) -> i - i.mod(lower.secondsPerPoint)
                    lowerIntervals = (fit(p[0]) for p in alignedPoints)
                    uniqueLowerIntervals = _.uniq(lowerIntervals)
                    for interval in uniqueLowerIntervals
                        propagateCalls.push {interval: interval, header: header, higher: higher, lower: lower}
                    higher = lower

                callPropagate = (args, callback) ->
                    propagate fd, args.interval, args.header.xFilesFactor, args.higher, args.lower, (err, result) ->
                        cb err if err
                        callback err, result

                #callPropagate()
                async.forEachSeries propagateCalls, callPropagate, (err, result) ->
                    throw err if err
                    console.log 'done dirty prop loop'
                    cb null
            else
                console.log 'prop1.end'
                cb null

info = (path, cb) ->
    # FIXME: Close this stream?
    # FIXME: Signal errors to callback?

    # FIXME: Stream parsing with node-binary dies
    # Looks like an issue, see their GitHub
    # Using fs.readFile() instead of read stream for now
    fs.readFile path, (err, data) ->
        cb err if err
        archives = []; metadata = {}

        Binary.parse(data)
            .word32bu('lastUpdate')
            .word32bu('maxRetention')
            .buffer('xff', 4) # Must decode separately since node-binary can't handle floats
            .word32bu('archiveCount')
            .tap (vars) ->
                metadata = vars
                metadata.xff = pack.Unpack('!f', vars.xff, 0)[0]
                @flush()
                for index in [0...metadata.archiveCount]
                    @word32bu('offset').word32bu('secondsPerPoint').word32bu('points')
                    @tap (archive) ->
                        @flush()
                        archive.retention = archive.secondsPerPoint * archive.points
                        archive.size = archive.points * POINT_SIZE
                        archives.push(archive)
            .tap ->
                cb null,
                    maxRetention: metadata.maxRetention
                    xFilesFactor: metadata.xff
                    archives: archives
    return

fetch = (path, from, to, cb) ->
    info path, (err, header) ->
        now = unixTime()
        oldestTime = now - header.maxRetention
        from = oldestTime if from < oldestTime
        throw new Error('Invalid time interval') unless from < to
        to = now if to > now or to < from
        diff = now - from
        fd = null

        # Find closest archive to look in, that iwll contain our information
        for archive in header.archives
            break if archive.retention >= diff

        fromInterval = parseInt(from - from.mod(archive.secondsPerPoint)) + archive.secondsPerPoint
        toInterval = parseInt(to - to.mod(archive.secondsPerPoint)) + archive.secondsPerPoint

        file = fs.createReadStream(path)

        Binary.stream(file)
            .skip(archive.offset)
            .word32bu('baseInterval')
            .word32bu('baseValue')
            .tap (vars) ->
                if vars.baseInterval == 0
                    # Nothing has been written to this hoard
                    step = archive.secondsPerPoint
                    points = (toInterval - fromInterval) / step
                    timeInfo = [fromInterval, toInterval, step]
                    values = (null for n in [0...points])
                    cb(null, timeInfo, values)
                else
                    #console.log 'vars', vars
                    #console.log 'fromInterval', fromInterval
                    #console.log 'toInterval', toInterval

                    #console.log '---------------'
                    # We have data in this hoard, let's read it
                    getOffset = (interval) ->
                        timeDistance = interval - vars.baseInterval
                        #console.log 'timeDistance', timeDistance
                        pointDistance = timeDistance / archive.secondsPerPoint
                        #console.log 'pointDistance', pointDistance
                        byteDistance = pointDistance * pointSize
                        #console.log 'byteDistance', byteDistance
                        a = archive.offset + byteDistance.mod(archive.size)
                        #console.log 'archive.offset', archive.offset
                        #console.log 'archive.size', archive.size
                        #console.log 'modulo', byteDistance.mod(archive.size)
                        a

                    fromOffset = getOffset(fromInterval)
                    toOffset = getOffset(toInterval)

                    #console.log 'fromOffset', fromOffset
                    #console.log 'toOffset', toOffset


                    fs.open path, 'r', (err, fd) ->
                        if err then throw err
                        if fromOffset < toOffset
                            #console.log 'NO WRAP'
                            # We don't wrap around, can everything in a single read
                            size = toOffset - fromOffset
                            seriesBuffer = new Buffer(size)
                            fs.read fd, seriesBuffer, 0, size, fromOffset, (err, num) ->
                                cb(err) if err
                                fs.close fd, (err) ->
                                    cb(err) if err
                                    unpack(seriesBuffer) # We have read it, go unpack!
                        else
                            # We wrap around the archive, we need two reads
                            archiveEnd = archive.offset + archive.size
                            size1 = archiveEnd - fromOffset
                            size2 = toOffset - archive.offset
                            seriesBuffer = new Buffer(size1 + size2)
                            fs.read fd, seriesBuffer, 0, size1, fromOffset, (err, num) ->
                                cb(err) if err
                                fs.read fd, seriesBuffer, size1, size2, archive.offset, (err, num) ->
                                    cb(err) if err
                                    unpack(seriesBuffer) # We have read it, go unpack!
                                    fs.close(fd)

        unpack = (seriesData) ->
            #console.log 'SERIES DATA', seriesData

            #console.log((f for f in seriesData).join(","))
            # Optmize this?
            numPoints = seriesData.length / POINT_SIZE
            #console.log 'SERIES LEN', numPoints
            seriesFormat = "!" + ('Ld' for f in [0...numPoints]).join("")
            #console.log 'SERIES FORMAT', seriesFormat
            unpackedSeries = pack.Unpack(seriesFormat, seriesData)
            #console.log 'SERIES UNPACKED', unpackedSeries

            # Use buffer/pre-allocate?
            valueList = (null for f in [0...numPoints])
            currentInterval = fromInterval
            step = archive.secondsPerPoint

            for i in [0...unpackedSeries.length] by 2
                pointTime = unpackedSeries[i]
                if pointTime == currentInterval
                    pointValue = unpackedSeries[i + 1]
                    valueList[i / 2] = pointValue
                currentInterval += step

            timeInfo = [fromInterval, toInterval, step]
            cb(null, timeInfo, valueList)
    return

exports.create = create
exports.update = update
exports.updateMany = updateMany
exports.info = info
exports.fetch = fetch