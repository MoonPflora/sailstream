package wrapper

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"sailstream/internal/config"
	"sailstream/internal/enviroment" // note: your package name has a typo
)

// ==================== TYPES ====================

type CNNTrainParams struct {
	DataDir         string `json:"data_dir"`
	ModelName       string `json:"model_name"`
	ModelsDir       string `json:"models_dir"`
	Epochs          int    `json:"epochs"`
	BatchSize       int    `json:"batch_size"`
	NumAngles       int    `json:"num_angles"`
	UseHybrid       *bool  `json:"use_hybrid"`
	UseAugmentation *bool  `json:"use_augmentation"`
	UseMultilingual *bool  `json:"use_multilingual"`
	BoostAccuracy   *bool  `json:"boost_accuracy"`
}

type CNNTrainResult struct {
	Success      bool                   `json:"success"`
	ModelPath    string                 `json:"model_path"`
	Accuracy     float64                `json:"accuracy"`
	NumClasses   int                    `json:"num_classes"`
	TrainingTime string                 `json:"training_time"`
	Logs         string                 `json:"logs"`
	Metrics      map[string]interface{} `json:"metrics"`
}

type CNNPredictParams struct {
	ModelPath  string   `json:"model_path"`
	ImagePath  string   `json:"image_path"`
	ImagePaths []string `json:"image_paths"`
	UseTTA     bool     `json:"use_tta"`
	TopK       int      `json:"top_k"`
}

type CNNPrediction struct {
	PID         string  `json:"pid"`
	Confidence  float64 `json:"confidence"`
	ClassIndex  int     `json:"class_index"`
	ProductName string  `json:"product_name"`
	ClassName   string  `json:"class_name"`
}

type CNNPredictResult struct {
	Success       bool            `json:"success"`
	Predictions   []CNNPrediction `json:"predictions"`
	TopPrediction *CNNPrediction  `json:"top_prediction"`
	Logs          string          `json:"logs"`
	InferenceTime string          `json:"inference_time"`
}

type CNNProductionTrainParams struct {
	BaseModel       string `json:"base_model"`
	NewDataDir      string `json:"new_data_dir"`
	OutputName      string `json:"output_name"`
	Strategy        string `json:"strategy"`
	Epochs          int    `json:"epochs"`
	BatchSize       int    `json:"batch_size"`
	UseAugmentation bool   `json:"use_augmentation"`
}

// ==================== WRAPPER ====================

type CNNWrapper struct {
	cfg         *config.Config
	env         *enviroment.Environment
	pythonPath  string
	cnnDir      string
	projectRoot string
}

func NewCNNWrapper(cfg *config.Config, env *enviroment.Environment) *CNNWrapper {
	if cfg == nil {
		panic("CNNWrapper: cfg must not be nil")
	}
	if env == nil {
		panic("CNNWrapper: env must not be nil")
	}

	w := &CNNWrapper{
		cfg:        cfg,
		env:        env,
		pythonPath: resolvePython(env),
	}
	w.resolvePaths()
	return w
}

func resolvePython(env *enviroment.Environment) string {
	if env.IsTermux() {
		termuxPy := "/data/data/com.termux/files/usr/bin/python3"
		if _, err := os.Stat(termuxPy); err == nil {
			return termuxPy
		}
	}
	for _, p := range []string{"python3", "python"} {
		if path, err := exec.LookPath(p); err == nil {
			return path
		}
	}
	return "python3"
}

func (w *CNNWrapper) resolvePaths() {
	w.projectRoot = w.env.GetDataDir()
	if w.projectRoot == "" {
		if cwd, err := os.Getwd(); err == nil {
			w.projectRoot = cwd
		} else {
			w.projectRoot = "."
		}
	}

	w.cnnDir = w.cfg.Paths.Models
	if w.cnnDir == "" {
		candidate := filepath.Join(w.projectRoot, "cnn")
		if _, err := os.Stat(candidate); err == nil {
			w.cnnDir = candidate
		} else {
			w.cnnDir = w.projectRoot
		}
	}
}

func (w *CNNWrapper) defaultEpochs(override int) int {
	if override > 0 {
		return override
	}
	if w.cfg.ImageRecognition.TrainingDefaults.Epochs > 0 {
		return w.cfg.ImageRecognition.TrainingDefaults.Epochs
	}
	return 30
}

func (w *CNNWrapper) defaultBatchSize(override int) int {
	if override > 0 {
		return override
	}
	if w.cfg.ImageRecognition.TrainingDefaults.BatchSize > 0 {
		return w.cfg.ImageRecognition.TrainingDefaults.BatchSize
	}
	return 8
}

func (w *CNNWrapper) defaultNumAngles(override int) int {
	if override > 0 {
		return override
	}
	if w.cfg.ImageRecognition.TrainingDefaults.NumAngles > 0 {
		return w.cfg.ImageRecognition.TrainingDefaults.NumAngles
	}
	return 3
}

func boolOrDefault(ptr *bool, def bool) bool {
	if ptr != nil {
		return *ptr
	}
	return def
}

// ==================== TRAIN ====================

func (w *CNNWrapper) Train(params CNNTrainParams) (*CNNTrainResult, error) {
	td := w.cfg.ImageRecognition.TrainingDefaults

	dataDir := params.DataDir
	if dataDir == "" {
		dataDir = w.cfg.Paths.TrainingImages
	}
	if dataDir == "" {
		return nil, fmt.Errorf("no data_dir provided and cfg.Paths.TrainingImages is empty")
	}
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("training data directory not found: %s", dataDir)
	}

	modelsDir := params.ModelsDir
	if modelsDir == "" {
		modelsDir = w.cfg.Paths.Models
	}
	if modelsDir == "" {
		modelsDir = filepath.Join(w.projectRoot, "models")
	}
	if err := os.MkdirAll(modelsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create models directory: %v", err)
	}

	useHybrid := boolOrDefault(params.UseHybrid, td.UseHybrid)
	useAugmentation := boolOrDefault(params.UseAugmentation, td.UseAugmentation)
	useMultilingual := boolOrDefault(params.UseMultilingual, td.UseMultilingual)
	boostAccuracy := boolOrDefault(params.BoostAccuracy, td.BoostAccuracy)

	epochs := w.defaultEpochs(params.Epochs)
	batchSize := w.defaultBatchSize(params.BatchSize)
	numAngles := w.defaultNumAngles(params.NumAngles)

	script := filepath.Join(w.cnnDir, "main_trainer.py")
	args := []string{
		script,
		"--train-new",
		"--data-dir", dataDir,
		"--output-dir", modelsDir,
		"--model-name", params.ModelName,
		"--epochs", fmt.Sprintf("%d", epochs),
		"--batch-size", fmt.Sprintf("%d", batchSize),
		"--num-angles", fmt.Sprintf("%d", numAngles),
		"--json",
	}
	if w.cfg.ImageRecognition.MaxImageSizePx > 0 {
		args = append(args, "--max-image-size", fmt.Sprintf("%d", w.cfg.ImageRecognition.MaxImageSizePx))
	}
	if useHybrid {
		args = append(args, "--use-hybrid")
	} else {
		args = append(args, "--use-basic")
	}
	if useAugmentation {
		args = append(args, "--augment")
	}

	cmd := exec.Command(w.pythonPath, args...)
	cmd.Dir = w.projectRoot
	output, err := cmd.CombinedOutput()

	result := &CNNTrainResult{
		Logs:    string(output),
		Metrics: make(map[string]interface{}),
	}
	if err != nil {
		result.Success = false
		return result, fmt.Errorf("training failed: %v\nOutput: %s", err, output)
	}
	if parseErr := parseJSONInto(output, result); parseErr == nil {
		result.Success = true
	}

	if useMultilingual {
		result.Logs += "\n" + w.RunMultilingualAugmentation(dataDir)
	}
	if boostAccuracy && result.ModelPath != "" {
		result.Logs += "\n" + w.RunAccuracyBooster(result.ModelPath)
	}

	result.Success = true
	return result, nil
}

// ==================== PRODUCTION TRAINING ====================

func (w *CNNWrapper) ProductionTrain(params CNNProductionTrainParams) (*CNNTrainResult, error) {
	if _, err := os.Stat(params.NewDataDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("training data directory not found: %s", params.NewDataDir)
	}
	if _, err := os.Stat(params.BaseModel); os.IsNotExist(err) {
		return nil, fmt.Errorf("base model not found: %s", params.BaseModel)
	}

	outputDir := filepath.Join(filepath.Dir(params.BaseModel), "production")
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %v", err)
	}

	epochs := w.defaultEpochs(params.Epochs)
	batchSize := w.defaultBatchSize(params.BatchSize)

	script := filepath.Join(w.cnnDir, "production_trainer.py")
	args := []string{
		script,
		"--base-model", params.BaseModel,
		"--new-data", params.NewDataDir,
		"--output-name", params.OutputName,
		"--strategy", params.Strategy,
		"--epochs", fmt.Sprintf("%d", epochs),
		"--batch-size", fmt.Sprintf("%d", batchSize),
		"--output-dir", outputDir,
		"--json",
	}
	if params.UseAugmentation {
		args = append(args, "--augment")
	}

	cmd := exec.Command(w.pythonPath, args...)
	cmd.Dir = w.projectRoot
	output, err := cmd.CombinedOutput()

	result := &CNNTrainResult{
		Logs:    string(output),
		Metrics: make(map[string]interface{}),
	}
	if err != nil {
		result.Success = false
		return result, fmt.Errorf("production training failed: %v\nOutput: %s", err, output)
	}
	if parseErr := parseJSONInto(output, result); parseErr == nil {
		result.Success = true
	}
	return result, nil
}

// ==================== PREDICT ====================

func (w *CNNWrapper) Predict(params CNNPredictParams) (*CNNPredictResult, error) {
	modelPath := params.ModelPath
	if modelPath == "" {
		modelPath = w.cfg.ImageRecognition.ModelPath
	}
	if modelPath == "" {
		return nil, fmt.Errorf("no model_path provided and cfg.ImageRecognition.ModelPath is empty")
	}
	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("model not found: %s", modelPath)
	}

	topK := params.TopK
	if topK == 0 {
		topK = 3
	}

	script := filepath.Join(w.cnnDir, "main_trainer.py")
	args := []string{
		script,
		"--predict",
		"--model", modelPath,
		"--top-k", fmt.Sprintf("%d", topK),
		"--json",
	}
	if w.cfg.ImageRecognition.MaxImageSizePx > 0 {
		args = append(args, "--max-image-size", fmt.Sprintf("%d", w.cfg.ImageRecognition.MaxImageSizePx))
	}

	imagePath := params.ImagePath
	if imagePath == "" && len(params.ImagePaths) > 0 {
		imagePath = params.ImagePaths[0]
	}
	if imagePath == "" {
		return nil, fmt.Errorf("no image path provided")
	}
	if _, err := os.Stat(imagePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("image not found: %s", imagePath)
	}
	args = append(args, "--image", imagePath)

	if params.UseTTA {
		args = append(args, "--use-tta")
	} else {
		args = append(args, "--no-tta")
	}

	cmd := exec.Command(w.pythonPath, args...)
	cmd.Dir = w.projectRoot
	output, err := cmd.CombinedOutput()

	result := &CNNPredictResult{Logs: string(output)}
	if err != nil {
		result.Success = false
		return result, fmt.Errorf("prediction failed: %v\nOutput: %s", err, output)
	}

	var data map[string]interface{}
	if jsonErr := json.Unmarshal(output, &data); jsonErr == nil {
		if t, ok := data["inference_time"].(string); ok {
			result.InferenceTime = t
		}
		if preds, ok := data["predictions"].([]interface{}); ok {
			threshold := w.cfg.ImageRecognition.ConfidenceThreshold
			for _, p := range preds {
				m, ok := p.(map[string]interface{})
				if !ok {
					continue
				}
				pred := CNNPrediction{
					PID:         cnnGetString(m, "pid", "product_id", "id"),
					Confidence:  cnnGetFloat(m, "confidence", "score"),
					ClassIndex:  int(cnnGetFloat(m, "class_index", "class_id")),
					ProductName: cnnGetString(m, "product_name", "class_name", "name"),
					ClassName:   cnnGetString(m, "class_name", "label"),
				}
				if pred.ProductName == "" {
					pred.ProductName = pred.ClassName
				}
				if threshold > 0 && pred.Confidence < threshold {
					continue
				}
				result.Predictions = append(result.Predictions, pred)
			}
		}
	}

	if len(result.Predictions) > 0 {
		result.TopPrediction = &result.Predictions[0]
	}
	result.Success = true
	return result, nil
}

// ==================== SUPPORT SCRIPTS ====================

func (w *CNNWrapper) RunMultilingualAugmentation(dataDir string) string {
	script := filepath.Join(w.cnnDir, "multilingual_augmentation.py")
	if _, err := os.Stat(script); os.IsNotExist(err) {
		return "multilingual_augmentation.py not found — skipping"
	}
	cmd := exec.Command(w.pythonPath, script, "--data-dir", dataDir, "--apply", "--json")
	cmd.Dir = w.projectRoot
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Sprintf("multilingual augmentation failed: %v — %s", err, out)
	}
	return fmt.Sprintf("multilingual augmentation: %s", string(out))
}

func (w *CNNWrapper) RunAccuracyBooster(modelPath string) string {
	script := filepath.Join(w.cnnDir, "accuracy_booster.py")
	if _, err := os.Stat(script); os.IsNotExist(err) {
		return "accuracy_booster.py not found — skipping"
	}
	cmd := exec.Command(w.pythonPath, script, "--model", modelPath, "--boost", "--json")
	cmd.Dir = w.projectRoot
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Sprintf("accuracy boosting failed: %v — %s", err, out)
	}
	return fmt.Sprintf("accuracy booster: %s", string(out))
}

// ==================== DELETE ====================

func (w *CNNWrapper) DeleteModel(modelPath string) (string, error) {
	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		return "", fmt.Errorf("model not found: %s", modelPath)
	}
	if err := os.Remove(modelPath); err != nil {
		return "", err
	}
	deleted := []string{modelPath}
	base := strings.TrimSuffix(modelPath, filepath.Ext(modelPath))
	for _, ext := range []string{".json", "_meta.json", "_metadata.json", ".tflite", "_labels.json", "_config.json"} {
		f := base + ext
		if _, err := os.Stat(f); err == nil {
			os.Remove(f)
			deleted = append(deleted, f)
		}
	}
	return fmt.Sprintf("deleted %d file(s)", len(deleted)), nil
}

// ==================== HELPERS ====================

func parseJSONInto(output []byte, r *CNNTrainResult) error {
	start := strings.Index(string(output), "{")
	if start < 0 {
		return fmt.Errorf("no JSON in output")
	}
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(string(output)[start:]), &data); err != nil {
		return err
	}
	if path, ok := data["model_path"].(string); ok {
		r.ModelPath = path
	}
	if acc, ok := data["final_accuracy"].(float64); ok {
		r.Accuracy = acc
	}
	if n, ok := data["num_classes"].(float64); ok {
		r.NumClasses = int(n)
	}
	if t, ok := data["training_time"].(string); ok {
		r.TrainingTime = t
	}
	r.Metrics = data
	return nil
}

func cnnGetString(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k].(string); ok {
			return v
		}
	}
	return ""
}

func cnnGetFloat(m map[string]interface{}, keys ...string) float64 {
	for _, k := range keys {
		if v, ok := m[k].(float64); ok {
			return v
		}
	}
	return 0
}