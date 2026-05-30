package com.kafka.active.web;

import com.kafka.active.failover.FailoverTestReport;
import com.kafka.active.failover.FailoverTestService;
import java.util.List;
import java.util.Map;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/failover/test")
@Profile("!test")
public class FailoverTestController {

	private final FailoverTestService testService;

	public FailoverTestController(FailoverTestService testService) {
		this.testService = testService;
	}

	@PostMapping("/start")
	public ResponseEntity<?> start(
			@RequestParam(required = false) String produceCluster) {
		try {
			return ResponseEntity.ok(testService.startRun(produceCluster));
		} catch (IllegalStateException e) {
			return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
		}
	}

	@PostMapping("/produce")
	public ResponseEntity<?> produce(
			@RequestParam(defaultValue = "20") int count,
			@RequestParam(defaultValue = "fo-verify") String prefix) {
		try {
			List<String> ids = testService.produceForRun(count, prefix);
			return ResponseEntity.ok(
					Map.of("produced", ids.size(), "sampleIds", ids.stream().limit(10).toList(), "report", testService.liveReport()));
		} catch (IllegalStateException e) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
		}
	}

	@PostMapping("/failover/standby")
	public ResponseEntity<?> failoverStandby() {
		try {
			return ResponseEntity.ok(testService.failoverToStandby());
		} catch (IllegalStateException e) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
		}
	}

	@PostMapping("/mirror-backlog-check")
	public ResponseEntity<?> mirrorBacklogCheck() {
		try {
			return ResponseEntity.ok(testService.checkMirrorBacklog());
		} catch (IllegalStateException e) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
		}
	}

	@PostMapping("/failback/primary")
	public ResponseEntity<?> failbackPrimary() {
		try {
			return ResponseEntity.ok(testService.failbackToPrimary());
		} catch (IllegalStateException e) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
		}
	}

	@PostMapping("/finish")
	public ResponseEntity<?> finish() {
		try {
			return ResponseEntity.ok(testService.finishRun());
		} catch (IllegalStateException e) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", e.getMessage()));
		}
	}

	@GetMapping("/active")
	public ResponseEntity<?> active() {
		FailoverTestReport r = testService.liveReport();
		if (r == null) {
			return ResponseEntity.ok(Map.of("active", false));
		}
		return ResponseEntity.ok(Map.of("active", true, "report", r));
	}
}
