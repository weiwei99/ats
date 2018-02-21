package disklayout

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/golang/glog"
	"github.com/weiwei99/ats/lib/cache"
)

// cache disk header parse
func (lo *Layout) parseCacheDiskHeader() error {
	cd := lo.CacheDisk

	// 加载基本信息
	buffer, err := cd.Dio.Read(cache.START, cd.HeaderLen)
	if err != nil {
		fmt.Println(err)
		return err
	}
	err = loadDiskHeaderFromBytes(buffer, cd.Header)
	if err != nil {
		fmt.Println(err)
		return err
	}

	// 预存数据
	if cd.DebugLoad {
		cd.PsRawDiskHeaderData = make([]byte, DiskHeaderLen)
		copy(cd.PsRawDiskHeaderData, buffer)
	}
	cd.PsDiskOffsetStart = int64(cache.START)
	cd.PsDiskOffsetEnd = int64(cache.START + DiskHeaderLen)
	return nil
}

// DiskHeader加载
func loadDiskHeaderFromBytes(buffer []byte, header *cache.DiskHeader) error {

	curPos := 0
	header.Magic = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	if header.Magic != DISK_HEADER_MAGIC {
		return fmt.Errorf("disk header magic not match")
	}
	glog.V(10).Infof(
		"CacheDisk - DiskHeader - Magic <Offset %d>: \n %s\n",
		curPos, hex.Dump(buffer[curPos:curPos+4]))
	curPos += 4

	header.NumVolumes = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	glog.V(10).Infof(
		"CacheDisk - DiskHeader - NumVolume <Offset %d>: \n %s\n",
		curPos, hex.Dump(buffer[curPos:curPos+4]))
	curPos += 4

	header.NumFree = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	glog.V(10).Infof(
		"CacheDisk - DiskHeader - NumFree <Offset %d>: \n %s\n",
		curPos, hex.Dump(buffer[curPos:curPos+4]))
	curPos += 4

	header.NumUsed = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	glog.V(10).Infof(
		"CacheDisk - DiskHeader - NumUsed <Offset %d>: \n %s\n",
		curPos, hex.Dump(buffer[curPos:curPos+4]))
	curPos += 4

	header.NumDiskvolBlks = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	glog.V(10).Infof(
		"CacheDisk - DiskHeader - NumDiskVolBlocks <Offset %d>: \n %s\n",
		curPos, hex.Dump(buffer[curPos:curPos+4]))
	curPos += 4

	// 因为C语言对齐
	curPos += 4
	header.NumBlocks = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	glog.V(10).Infof(
		"CacheDisk - DiskHeader - NumBlocks <Offset %d>: \n %s\n",
		curPos, hex.Dump(buffer[curPos:curPos+8]))
	curPos += 8
	//uint64_t delta_3_2 = skip - (skip >> STORE_BLOCK_SHIFT);

	// 对齐
	//curPos += 2
	if len(header.VolInfos) < int(header.NumVolumes) {
		return fmt.Errorf("vol info space not enough")
	}

	for i := 0; i < int(header.NumVolumes); i++ {
		header.VolInfos[i].Offset = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
		glog.V(10).Infof(
			"CacheDisk - DiskHeader - VolInfo - Offset <Offset %d>: \n %s\n",
			curPos, hex.Dump(buffer[curPos:curPos+8]))
		curPos += 8

		header.VolInfos[i].Len = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
		glog.V(10).Infof(
			"CacheDisk - DiskHeader - VolInfo - Len <Offset %d>: \n %s\n",
			curPos, hex.Dump(buffer[curPos:curPos+8]))
		curPos += 8

		header.VolInfos[i].Number = int(binary.LittleEndian.Uint32(buffer[curPos : curPos+4]))
		glog.V(10).Infof(
			"CacheDisk - DiskHeader - VolInfo - Number <Offset %d>: \n %s\n",
			curPos, hex.Dump(buffer[curPos:curPos+4]))
		curPos += 4

		// binary.LittleEndian.Uint16

		bytesValue := binary.LittleEndian.Uint16(buffer[curPos : curPos+2])
		glog.V(10).Infof(
			"CacheDisk - DiskHeader - VolInfo - Type[0-3] & Free[4-4] <Offset %d>: \n %s\n",
			curPos, hex.Dump(buffer[curPos:curPos+2]))
		header.VolInfos[i].Type = uint8(bytesValue & 0x0007)
		header.VolInfos[i].Free = uint8(bytesValue & 0x0008)

		curPos += 4
	}

	return nil
}
