require File.expand_path('lib/kroniko/version', __dir__)

Gem::Specification.new do |spec|
  spec.name          = "kroniko"
  spec.version       = Kroniko::VERSION
  spec.authors       = ["Manuel Ortega"]
  spec.email         = ["maoc84@gmail.com"]

  spec.summary       = "A file-based DCB compatible Event Store"
  spec.description   = "A file-based DCB compatible Event Store for Ruby"
  spec.homepage      = "https://github.com/ortegacmanuel/kroniko"
  spec.license       = "MIT"

  spec.files         = Dir["lib/**/*.rb"]
  spec.require_paths = ["lib"]

  spec.add_dependency "json"
  spec.add_dependency "fileutils"
  spec.add_dependency "securerandom"
  spec.add_dependency "thread"
end
