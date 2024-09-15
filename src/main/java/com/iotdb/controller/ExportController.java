package com.iotdb.controller;

import com.iotdb.config.FileStorageProperties;
import com.iotdb.dto.QueryDto;
import com.iotdb.exception.ServiceException;
import com.iotdb.service.ExportService;
import com.iotdb.vo.Result;
import org.apache.ratis.thirdparty.com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.UriUtils;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static com.iotdb.enums.StatusCodeEnum.FAIL;

/**
 * @author tjb
 * @date 2024/8/2
 */
@RestController
@RequestMapping
public class ExportController {
    @Resource
    private ExportService exportService;
    @Resource
    private FileStorageProperties fileStorageProperties;
    private final RateLimiter rateLimiter = RateLimiter.create(1);

    @PostMapping("/exportData")
    public Result<?> exportData(@RequestBody QueryDto queryDto){
        boolean tryAcquire = rateLimiter.tryAcquire(500, TimeUnit.MILLISECONDS);
        if (!tryAcquire) {
            throw new ServiceException(FAIL.getCode(), "Too many request in this method");
        }
        exportService.export(queryDto);
        return Result.ok("", "任务提交成功");
    }



    @GetMapping("/download/{filename}")
    public ResponseEntity<InputStreamResource> downloadFile(@PathVariable("filename") String filename) throws IOException {
        boolean tryAcquire = rateLimiter.tryAcquire(500, TimeUnit.MILLISECONDS);
        if (!tryAcquire) {
            throw new ServiceException(FAIL.getCode(), "Too many request in this method");
        }
        Path filePath = Paths.get(fileStorageProperties.getStorage_path(), filename);
        File file = filePath.toFile();

        if (!file.exists()) {
            throw new ServiceException(FAIL.getCode(), "文件不存在,检查后重试");
        }

        // 获取文件内容类型
        String contentType = Files.probeContentType(filePath);
        if (contentType == null) {
            contentType = "application/octet-stream";
        }

        // 设置响应头
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + UriUtils.encode(file.getName(), "UTF-8") + "\"");

        // 使用 InputStreamResource 包装文件输入流
        InputStreamResource resource = new InputStreamResource(Files.newInputStream(file.toPath()));

        // 返回 ResponseEntity
        return ResponseEntity.ok()
                .headers(headers)
                .contentType(MediaType.parseMediaType(contentType))
                .contentLength(file.length())
                .body(resource);
    }

}
