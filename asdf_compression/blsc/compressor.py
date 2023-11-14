from asdf.extension import Compressor


class BloscCompressor(Compressor):
    label = b"blsc"

    def compress(self, data, **kwargs):
        import blosc

        # Type size, necessary for shuffle filters
        typesize = data.itemsize

        # Compression level
        clevel = kwargs.get("level", 9)

        # Shuffle filter
        shuffle_string = kwargs.get("filter", "bitshuffle")
        if shuffle_string == "noshuffle":
            shuffle = blosc.NOSHUFFLE
        elif shuffle_string == "byteshuffle":
            shuffle = blosc.BYTESHUFFLE
        else:
            # Fallback and default
            shuffle = blosc.BITSHUFFLE

        # CODEC name
        cname = kwargs.get("codec", "blosclz")

        # Number of threads
        nthreads = kwargs.get("threads", 1)
        self._api.set_nthreads(nthreads)

        yield blosc.compress(data, typesize=typesize, clevel=clevel, shuffle=shuffle, cname=cname, **kwargs)

    def decompress(self, data, out, **kwargs):
        import blosc

        # TODO: call `self._api.decompress_ptr` instead to avoid copying the output
        out[:] = blosc.decompress(b"".join(data), **kwargs)
        return out.nbytes
