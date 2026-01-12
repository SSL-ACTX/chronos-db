#[derive(Debug, Clone, Copy)]
pub enum Metric {
    Euclidean,
    Cosine,
}

impl Metric {
    /// Calculate distance. LOWER is ALWAYS closer/better.
    /// Optimized: Returns SQUARED distance for Euclidean to avoid expensive sqrt().
    ///
    /// Unrolling 16 lanes allows LLVM to utilize full AVX-512 ZMM registers (512-bit).
    /// On older hardware (AVX2), this splits cleanly into 2 instructions (256-bit x2).
    #[inline(always)]
    pub fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            Metric::Euclidean => {
                let mut sum = 0.0;

                // Unroll 16 (AVX-512 optimal width for f32)
                let chunks = a.chunks_exact(16);
                let b_chunks = b.chunks_exact(16);

                // Remainder handling start index
                let remainder_start = a.len() - a.len() % 16;

                for (ac, bc) in chunks.zip(b_chunks) {
                    let d0  = ac[0]  - bc[0];
                    let d1  = ac[1]  - bc[1];
                    let d2  = ac[2]  - bc[2];
                    let d3  = ac[3]  - bc[3];
                    let d4  = ac[4]  - bc[4];
                    let d5  = ac[5]  - bc[5];
                    let d6  = ac[6]  - bc[6];
                    let d7  = ac[7]  - bc[7];
                    let d8  = ac[8]  - bc[8];
                    let d9  = ac[9]  - bc[9];
                    let d10 = ac[10] - bc[10];
                    let d11 = ac[11] - bc[11];
                    let d12 = ac[12] - bc[12];
                    let d13 = ac[13] - bc[13];
                    let d14 = ac[14] - bc[14];
                    let d15 = ac[15] - bc[15];

                    sum += d0*d0   + d1*d1   + d2*d2   + d3*d3   +
                    d4*d4   + d5*d5   + d6*d6   + d7*d7   +
                    d8*d8   + d9*d9   + d10*d10 + d11*d11 +
                    d12*d12 + d13*d13 + d14*d14 + d15*d15;
                }

                // Handle remainder
                for i in remainder_start..a.len() {
                    let diff = a[i] - b[i];
                    sum += diff * diff;
                }

                sum
            }
            Metric::Cosine => {
                let mut dot = 0.0;
                let mut norm_a = 0.0;
                let mut norm_b = 0.0;

                // Keep 8 for Cosine to balance register pressure with unrolling
                let chunks = a.chunks_exact(8);
                let b_chunks = b.chunks_exact(8);
                let remainder_start = a.len() - a.len() % 8;

                for (ac, bc) in chunks.zip(b_chunks) {
                    // Accumulate; compiler vectorizes FMA (Fused Multiply Add)
                    dot += ac[0]*bc[0] + ac[1]*bc[1] + ac[2]*bc[2] + ac[3]*bc[3] +
                    ac[4]*bc[4] + ac[5]*bc[5] + ac[6]*bc[6] + ac[7]*bc[7];

                    norm_a += ac[0]*ac[0] + ac[1]*ac[1] + ac[2]*ac[2] + ac[3]*ac[3] +
                    ac[4]*ac[4] + ac[5]*ac[5] + ac[6]*ac[6] + ac[7]*ac[7];

                    norm_b += bc[0]*bc[0] + bc[1]*bc[1] + bc[2]*bc[2] + bc[3]*bc[3] +
                    bc[4]*bc[4] + bc[5]*bc[5] + bc[6]*bc[6] + bc[7]*bc[7];
                }

                for i in remainder_start..a.len() {
                    dot += a[i] * b[i];
                    norm_a += a[i] * a[i];
                    norm_b += b[i] * b[i];
                }

                if norm_a == 0.0 || norm_b == 0.0 { return 1.0; }
                1.0 - (dot / (norm_a.sqrt() * norm_b.sqrt()))
            }
        }
    }
}
