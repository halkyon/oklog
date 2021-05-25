package store

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"time"
)

type DCSReader struct {
	log     *fileLogDCS
	qp      QueryParams
	timeout time.Duration
}

func NewDCSReader(log *fileLogDCS, qp QueryParams, timeout time.Duration) *DCSReader {
	return &DCSReader{log: log, qp: qp, timeout: downloadTimeout}
}

func (r *DCSReader) Read(b []byte) (int, error) {
	// todo
	return 0, nil
}

// used for a more efficient io.Copy. Write to a given writer directly without the need for a buffer.
func (r *DCSReader) WriteTo(w io.Writer) (n int64, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	var totalBytes int64

	// TODO: sorting the segment files from the bucket, an equivalent to sorting in queryMatchingSegments()
	iterator := r.log.project.ListObjects(ctx, r.log.bucketName, nil)
	for iterator.Next() {
		low, high, err := parseFilename(iterator.Item().Key)
		if err != nil {
			r.log.reportDCSWarning(err, iterator.Item().Key)
			continue
		}

		if !overlap(r.qp.From.ULID, r.qp.To.ULID, low, high) {
			continue
		}

		download, err := r.log.project.DownloadObject(ctx, r.log.bucketName, iterator.Item().Key, nil)
		if err != nil {
			r.log.reportDCSError(err, iterator.Item().Key)
			continue
		}

		gzDownload, err := gzip.NewReader(download)
		if err != nil {
			download.Close()
			r.log.reportDCSError(err, iterator.Item().Key)
			continue
		}

		n, err := io.Copy(w, gzDownload)
		if err != nil {
			gzDownload.Close()
			download.Close()
			r.log.reportDCSError(err, iterator.Item().Key)
			continue
		}

		gzDownload.Close()
		download.Close()

		totalBytes += n
	}

	r.log.reporter.ReportEvent(Event{
		Debug: true,
		Op:    "DCSReader",
		Msg:   fmt.Sprintf("Downloaded %d bytes from DCS", totalBytes),
	})

	return totalBytes, err
}
